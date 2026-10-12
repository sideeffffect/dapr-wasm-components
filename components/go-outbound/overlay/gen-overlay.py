#!/usr/bin/env python3
"""Generate the `go build -overlay` used to build go-outbound.

Patches Go's std library for wasip1 components. Each patch is a small
asserted string replacement on a file copied from $(go env GOROOT)/src; each
must match exactly once, so a Go upgrade that changes the patched code fails
loudly instead of silently.

- net: real TCP via wasi:sockets instead of wasip1's in-process fake network
  (calls the hooks in net/zz_wasisock.go, which is added to the package).
- runtime/netpoll_wasip1.go: a zero-timeout poll of no file descriptors
  returns without calling poll_oneoff. The GC's start-the-world does such a
  poll, and a GC can start inside cabi_realloc, where the component model
  forbids import calls ("cannot leave component instance").

Writes <out>/overlay.json (default: ../target/overlay).
"""
import json, os, subprocess, sys

here = os.path.dirname(os.path.abspath(__file__))
out = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else os.path.join(here, "..", "target", "overlay"))
goroot = subprocess.check_output(["go", "env", "GOROOT"], text=True).strip()
src = os.path.join(goroot, "src")

PATCHES = {
    "runtime/netpoll_wasip1.go": [
        ("""	if len(pollsubs) == 0 {
		unlock(&mtx)
		return gList{}, 0
	}
""", """	if len(pollsubs) == 0 {
		unlock(&mtx)
		return gList{}, 0
	}

	// dapr-wasm-components: only the timeout clock and no timeout: nothing
	// to poll. Skip the poll_oneoff import call, which is forbidden when
	// this runs from a GC inside cabi_realloc.
	if delay == 0 && len(pollsubs) == 1 {
		unlock(&mtx)
		return gList{}, 0
	}
"""),
    ],
    "net/fd_fake.go": [
        ("\t*fakeNetFD\n}", "\t*fakeNetFD\n\twasi *wasiFD\n}"),
        ("func (fd *netFD) accept() (netfd *netFD, err error) {\n",
         "func (fd *netFD) accept() (netfd *netFD, err error) {\n\tif fd.wasi != nil {\n\t\treturn fd.wasi.accept(fd)\n\t}\n"),
        ("func (fd *netFD) Close() error {\n",
         "func (fd *netFD) Close() error {\n\tif fd.wasi != nil {\n\t\treturn fd.wasi.Close()\n\t}\n"),
        ("func (fd *netFD) shutdown(how int) error {\n",
         "func (fd *netFD) shutdown(how int) error {\n\tif fd.wasi != nil && fd.wasi.conn != nil && how == syscall.SHUT_WR {\n\t\treturn fd.wasi.conn.CloseWrite()\n\t}\n\tif fd.wasi != nil {\n\t\treturn nil\n\t}\n"),
        ("func (fd *netFD) Read(p []byte) (n int, err error) {\n",
         "func (fd *netFD) Read(p []byte) (n int, err error) {\n\tif fd.wasi != nil {\n\t\treturn fd.wasi.conn.Read(p)\n\t}\n"),
        ("func (fd *netFD) Write(p []byte) (nn int, err error) {\n",
         "func (fd *netFD) Write(p []byte) (nn int, err error) {\n\tif fd.wasi != nil {\n\t\treturn fd.wasi.conn.Write(p)\n\t}\n"),
        ("func (fd *netFD) SetDeadline(t time.Time) error {\n",
         "func (fd *netFD) SetDeadline(t time.Time) error {\n\tif fd.wasi != nil {\n\t\tfd.wasi.SetReadDeadline(t)\n\t\treturn fd.wasi.SetWriteDeadline(t)\n\t}\n"),
        ("func (fd *netFD) SetReadDeadline(t time.Time) error {\n",
         "func (fd *netFD) SetReadDeadline(t time.Time) error {\n\tif fd.wasi != nil {\n\t\treturn fd.wasi.SetReadDeadline(t)\n\t}\n"),
        ("func (fd *netFD) SetWriteDeadline(t time.Time) error {\n",
         "func (fd *netFD) SetWriteDeadline(t time.Time) error {\n\tif fd.wasi != nil {\n\t\treturn fd.wasi.SetWriteDeadline(t)\n\t}\n"),
        ("import (\n", "import (\n\t\"syscall\"\n"),
    ],
    "net/net_fake.go": [
        ("ctrlCtxFn func(context.Context, string, string, syscall.RawConn) error) (*netFD, error) {\n",
         "ctrlCtxFn func(context.Context, string, string, syscall.RawConn) error) (*netFD, error) {\n\tif wasiEnabled(family, sotype) {\n\t\treturn wasiSocket(ctx, net, family, sotype, laddr, raddr)\n\t}\n"),
    ],
    "net/lookup_unix.go": [
        ("func (r *Resolver) lookupHost(ctx context.Context, host string) (addrs []string, err error) {\n",
         "func (r *Resolver) lookupHost(ctx context.Context, host string) (addrs []string, err error) {\n\tif wasiLookupHook != nil {\n\t\tips, err := wasiLookupIP(ctx, host)\n\t\tfor _, ip := range ips {\n\t\t\taddrs = append(addrs, ip.String())\n\t\t}\n\t\treturn addrs, err\n\t}\n"),
        ("func (r *Resolver) lookupIP(ctx context.Context, network, host string) (addrs []IPAddr, err error) {\n",
         "func (r *Resolver) lookupIP(ctx context.Context, network, host string) (addrs []IPAddr, err error) {\n\tif wasiLookupHook != nil {\n\t\treturn wasiLookupIP(ctx, host)\n\t}\n"),
    ],
}

os.makedirs(out, exist_ok=True)
replace = {}
for name, patches in PATCHES.items():
    text = open(os.path.join(src, name)).read()
    for old, new in patches:
        n = text.count(old)
        if n != 1:
            sys.exit(f"gen-overlay: {name}: expected 1 match, found {n}: {old!r}")
        text = text.replace(old, new)
    dst = os.path.join(out, name)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    open(dst, "w").write(text)
    replace[os.path.join(src, name)] = dst
replace[os.path.join(src, "net", "zz_wasisock.go")] = os.path.join(here, "net", "zz_wasisock.go")
json.dump({"Replace": replace}, open(os.path.join(out, "overlay.json"), "w"), indent=1)
print(os.path.join(out, "overlay.json"))
