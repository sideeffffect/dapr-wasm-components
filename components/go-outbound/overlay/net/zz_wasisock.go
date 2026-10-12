// Added to package net by `go build -overlay` (dapr-wasm-components go
// family). Lets a user package plug real TCP sockets (wasi:sockets) into the
// wasip1 build of net, whose built-in network is an in-process fake.

//go:build wasip1

package net

import (
	"context"
	"syscall"
	"time"
	_ "unsafe" // go:linkname
)

// wasiConn is a connected stream socket provided by the hook implementation.
type wasiConn interface {
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Close() error
	CloseWrite() error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}

// wasiListener is a listening stream socket provided by the hook implementation.
type wasiListener interface {
	// Accept returns a wasiConn plus the remote IP and port.
	Accept(deadline time.Time) (any, []byte, int, error)
	Close() error
}

var (
	wasiDialHook   func(ctx context.Context, ip []byte, port int) (conn any, localIP []byte, localPort int, err error)
	wasiListenHook func(ip []byte, port int) (ln any, localIP []byte, localPort int, err error)
	wasiLookupHook func(ctx context.Context, host string) ([][]byte, error)
)

// wasiSetHooks is called (via go:linkname) by the wasi:sockets implementation.
//
//go:linkname wasiSetHooks
func wasiSetHooks(dial, listen, lookup any) {
	wasiDialHook = dial.(func(context.Context, []byte, int) (any, []byte, int, error))
	wasiListenHook = listen.(func([]byte, int) (any, []byte, int, error))
	wasiLookupHook = lookup.(func(context.Context, string) ([][]byte, error))
}

type wasiFD struct {
	conn         wasiConn
	ln           wasiListener
	readDeadline time.Time
}

func wasiEnabled(family, sotype int) bool {
	return wasiDialHook != nil && sotype == syscall.SOCK_STREAM &&
		(family == syscall.AF_INET || family == syscall.AF_INET6)
}

func wasiTCPAddr(ip []byte, port int) *TCPAddr {
	return &TCPAddr{IP: IP(ip), Port: port}
}

func wasiSocket(ctx context.Context, net string, family, sotype int, laddr, raddr sockaddr) (*netFD, error) {
	fd := &netFD{family: family, sotype: sotype, net: net}
	if raddr == nil {
		var ip IP
		port := 0
		if a, ok := laddr.(*TCPAddr); ok && a != nil {
			ip, port = a.IP, a.Port
		}
		if ip == nil {
			if family == syscall.AF_INET6 {
				ip = IPv6unspecified
			} else {
				ip = IPv4zero
			}
		}
		if family == syscall.AF_INET {
			ip = ip.To4()
		}
		l, lip, lport, err := wasiListenHook(ip, port)
		if err != nil {
			return nil, &OpError{Op: "listen", Net: net, Addr: laddr, Err: err}
		}
		fd.wasi = &wasiFD{ln: l.(wasiListener)}
		fd.laddr = wasiTCPAddr(lip, lport)
		return fd, nil
	}
	ra, ok := raddr.(*TCPAddr)
	if !ok {
		return nil, &OpError{Op: "dial", Net: net, Addr: raddr, Err: syscall.EINVAL}
	}
	ip := ra.IP
	if ip4 := ip.To4(); ip4 != nil && family == syscall.AF_INET {
		ip = ip4
	}
	c, lip, lport, err := wasiDialHook(ctx, ip, ra.Port)
	if err != nil {
		return nil, err
	}
	fd.wasi = &wasiFD{conn: c.(wasiConn)}
	fd.laddr = wasiTCPAddr(lip, lport)
	fd.raddr = ra
	fd.isConnected = true
	return fd, nil
}

func (w *wasiFD) accept(fd *netFD) (*netFD, error) {
	if w.ln == nil {
		return nil, syscall.EINVAL
	}
	c, rip, rport, err := w.ln.Accept(w.readDeadline)
	if err != nil {
		return nil, err
	}
	return &netFD{
		family:      fd.family,
		sotype:      fd.sotype,
		net:         fd.net,
		laddr:       fd.laddr,
		raddr:       wasiTCPAddr(rip, rport),
		isConnected: true,
		wasi:        &wasiFD{conn: c.(wasiConn)},
	}, nil
}

func (w *wasiFD) Close() error {
	if w.ln != nil {
		return w.ln.Close()
	}
	return w.conn.Close()
}

func (w *wasiFD) SetReadDeadline(t time.Time) error {
	w.readDeadline = t
	if w.conn != nil {
		return w.conn.SetReadDeadline(t)
	}
	return nil
}

func (w *wasiFD) SetWriteDeadline(t time.Time) error {
	if w.conn != nil {
		return w.conn.SetWriteDeadline(t)
	}
	return nil
}

func wasiLookupIP(ctx context.Context, host string) ([]IPAddr, error) {
	ips, err := wasiLookupHook(ctx, host)
	if err != nil {
		return nil, &DNSError{Err: err.Error(), Name: host, IsNotFound: true}
	}
	out := make([]IPAddr, 0, len(ips))
	for _, ip := range ips {
		out = append(out, IPAddr{IP: IP(ip)})
	}
	return out, nil
}
