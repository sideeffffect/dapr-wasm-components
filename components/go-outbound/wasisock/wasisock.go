//go:build wasip1

// Package wasisock gives Go's net package real TCP sockets on wasm: it
// implements the hooks that the net overlay (netoverlay/) adds to package net
// on top of wasi:sockets@0.2. Import it for its side effect.
//
// Go on wasm runs every goroutine on one thread, so a wasi:io blocking call
// would freeze them all. Every wait is therefore cooperative: a non-blocking
// attempt, and when it would block, a short time.Sleep (1 ms doubling to
// 10 ms) that lets the Go scheduler run other goroutines.
package wasisock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"
	_ "unsafe" // go:linkname

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wasi_io_streams"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wasi_sockets_instance_network"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wasi_sockets_ip_name_lookup"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wasi_sockets_network"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wasi_sockets_tcp"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/wasi_sockets_tcp_create_socket"

	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

//go:linkname wasiSetHooks net.wasiSetHooks
func wasiSetHooks(dial, listen, lookup any)

func init() {
	wasiSetHooks(dial, listen, lookup)
}

const (
	minBackoff = time.Millisecond
	maxBackoff = 10 * time.Millisecond
)

type backoff time.Duration

func (b *backoff) wait() {
	if *b == 0 {
		*b = backoff(minBackoff)
	}
	time.Sleep(time.Duration(*b))
	if *b *= 2; *b > backoff(maxBackoff) {
		*b = backoff(maxBackoff)
	}
}

var network *wasi_sockets_network.Network

func instanceNetwork() *wasi_sockets_network.Network {
	if network == nil {
		network = wasi_sockets_instance_network.InstanceNetwork()
	}
	return network
}

// sockErr maps a wasi:sockets error-code to the closest syscall errno.
func sockErr(op string, code wasi_sockets_network.ErrorCode) error {
	var errno syscall.Errno
	switch code {
	case wasi_sockets_network.ErrorCodeAccessDenied:
		errno = syscall.EACCES
	case wasi_sockets_network.ErrorCodeNotSupported:
		errno = syscall.ENOTSUP
	case wasi_sockets_network.ErrorCodeInvalidArgument:
		errno = syscall.EINVAL
	case wasi_sockets_network.ErrorCodeOutOfMemory:
		errno = syscall.ENOMEM
	case wasi_sockets_network.ErrorCodeTimeout:
		errno = syscall.ETIMEDOUT
	case wasi_sockets_network.ErrorCodeAddressInUse:
		errno = syscall.EADDRINUSE
	case wasi_sockets_network.ErrorCodeAddressNotBindable:
		errno = syscall.EADDRNOTAVAIL
	case wasi_sockets_network.ErrorCodeRemoteUnreachable:
		errno = syscall.EHOSTUNREACH
	case wasi_sockets_network.ErrorCodeConnectionRefused:
		errno = syscall.ECONNREFUSED
	case wasi_sockets_network.ErrorCodeConnectionReset:
		errno = syscall.ECONNRESET
	case wasi_sockets_network.ErrorCodeConnectionAborted:
		errno = syscall.ECONNABORTED
	case wasi_sockets_network.ErrorCodeNameUnresolvable:
		return fmt.Errorf("%s: name unresolvable", op)
	default:
		return fmt.Errorf("%s: wasi:sockets error-code %d", op, code)
	}
	return os.NewSyscallError(op, errno)
}

func toSocketAddress(ip []byte, port int) wasi_sockets_network.IpSocketAddress {
	if len(ip) == 4 {
		return wasi_sockets_network.MakeIpSocketAddressIpv4(wasi_sockets_network.Ipv4SocketAddress{
			Port:    uint16(port),
			Address: witTypes.Tuple4[uint8, uint8, uint8, uint8]{F0: ip[0], F1: ip[1], F2: ip[2], F3: ip[3]},
		})
	}
	var s [8]uint16
	for i := range s {
		s[i] = uint16(ip[2*i])<<8 | uint16(ip[2*i+1])
	}
	return wasi_sockets_network.MakeIpSocketAddressIpv6(wasi_sockets_network.Ipv6SocketAddress{
		Port:    uint16(port),
		Address: witTypes.Tuple8[uint16, uint16, uint16, uint16, uint16, uint16, uint16, uint16]{F0: s[0], F1: s[1], F2: s[2], F3: s[3], F4: s[4], F5: s[5], F6: s[6], F7: s[7]},
	})
}

func fromIPAddress(a wasi_sockets_network.IpAddress) []byte {
	if a.Tag() == wasi_sockets_network.IpAddressIpv4 {
		v := a.Ipv4()
		return []byte{v.F0, v.F1, v.F2, v.F3}
	}
	v := a.Ipv6()
	s := []uint16{v.F0, v.F1, v.F2, v.F3, v.F4, v.F5, v.F6, v.F7}
	out := make([]byte, 16)
	for i, x := range s {
		out[2*i], out[2*i+1] = byte(x>>8), byte(x)
	}
	return out
}

func fromSocketAddress(a wasi_sockets_network.IpSocketAddress) ([]byte, int) {
	if a.Tag() == wasi_sockets_network.IpSocketAddressIpv4 {
		v := a.Ipv4()
		return []byte{v.Address.F0, v.Address.F1, v.Address.F2, v.Address.F3}, int(v.Port)
	}
	v := a.Ipv6()
	return fromIPAddress(wasi_sockets_network.MakeIpAddressIpv6(v.Address)), int(v.Port)
}

func family(ip []byte) wasi_sockets_network.IpAddressFamily {
	if len(ip) == 4 {
		return wasi_sockets_network.IpAddressFamilyIpv4
	}
	return wasi_sockets_network.IpAddressFamilyIpv6
}

// virtualConn adapts an in-memory net.Conn (e.g. one end of net.Pipe) to
// the connection shape the net overlay expects.
type virtualConn struct{ net.Conn }

func (virtualConn) CloseWrite() error { return nil }

var virtual = map[string]func() net.Conn{}

// RegisterVirtual makes dials to ip:port return dial() — an in-process
// connection — instead of a socket. Used for the app port, which Dapr
// probes but the app, inside this same component, does not listen on.
func RegisterVirtual(ip net.IP, port int, dial func() net.Conn) {
	virtual[(&net.TCPAddr{IP: ip, Port: port}).String()] = dial
}

func dial(ctx context.Context, ip []byte, port int) (any, []byte, int, error) {
	if v, ok := virtual[(&net.TCPAddr{IP: net.IP(ip), Port: port}).String()]; ok {
		return virtualConn{v()}, []byte{127, 0, 0, 1}, 0, nil
	}
	created := wasi_sockets_tcp_create_socket.CreateTcpSocket(family(ip))
	if created.IsErr() {
		return nil, nil, 0, sockErr("socket", created.Err())
	}
	sock := created.Ok()
	if r := sock.StartConnect(instanceNetwork(), toSocketAddress(ip, port)); r.IsErr() {
		sock.Drop()
		return nil, nil, 0, sockErr("connect", r.Err())
	}
	var b backoff
	for {
		r := sock.FinishConnect()
		if r.IsOk() {
			streams := r.Ok()
			lip, lport := []byte(nil), 0
			if la := sock.LocalAddress(); la.IsOk() {
				lip, lport = fromSocketAddress(la.Ok())
			}
			return newConn(sock, streams.F0, streams.F1), lip, lport, nil
		}
		if r.Err() != wasi_sockets_network.ErrorCodeWouldBlock {
			sock.Drop()
			return nil, nil, 0, sockErr("connect", r.Err())
		}
		if err := ctx.Err(); err != nil {
			sock.Drop()
			return nil, nil, 0, err
		}
		b.wait()
	}
}

func listen(ip []byte, port int) (any, []byte, int, error) {
	created := wasi_sockets_tcp_create_socket.CreateTcpSocket(family(ip))
	if created.IsErr() {
		return nil, nil, 0, sockErr("socket", created.Err())
	}
	sock := created.Ok()
	fail := func(op string, code wasi_sockets_network.ErrorCode) (any, []byte, int, error) {
		sock.Drop()
		return nil, nil, 0, sockErr(op, code)
	}
	if r := sock.StartBind(instanceNetwork(), toSocketAddress(ip, port)); r.IsErr() {
		return fail("bind", r.Err())
	}
	var b backoff
	for r := sock.FinishBind(); r.IsErr(); r = sock.FinishBind() {
		if r.Err() != wasi_sockets_network.ErrorCodeWouldBlock {
			return fail("bind", r.Err())
		}
		b.wait()
	}
	if r := sock.StartListen(); r.IsErr() {
		return fail("listen", r.Err())
	}
	b = 0
	for r := sock.FinishListen(); r.IsErr(); r = sock.FinishListen() {
		if r.Err() != wasi_sockets_network.ErrorCodeWouldBlock {
			return fail("listen", r.Err())
		}
		b.wait()
	}
	lip, lport := ip, port
	if la := sock.LocalAddress(); la.IsOk() {
		lip, lport = fromSocketAddress(la.Ok())
	}
	return &listener{sock: sock}, lip, lport, nil
}

func lookup(ctx context.Context, host string) ([][]byte, error) {
	r := wasi_sockets_ip_name_lookup.ResolveAddresses(instanceNetwork(), host)
	if r.IsErr() {
		return nil, sockErr("lookup "+host, r.Err())
	}
	stream := r.Ok()
	defer stream.Drop()
	var out [][]byte
	var b backoff
	for {
		next := stream.ResolveNextAddress()
		if next.IsErr() {
			if next.Err() == wasi_sockets_network.ErrorCodeWouldBlock {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				b.wait()
				continue
			}
			return nil, sockErr("lookup "+host, next.Err())
		}
		addr := next.Ok()
		if addr.IsNone() {
			break
		}
		ip := fromIPAddress(addr.Some())
		// Prefer IPv4 first: dialers try addresses in order.
		if len(ip) == 4 {
			out = append([][]byte{ip}, out...)
		} else {
			out = append(out, ip)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("lookup %s: no addresses", host)
	}
	return out, nil
}

type listener struct {
	sock   *wasi_sockets_tcp.TcpSocket
	closed bool
}

func (l *listener) Accept(deadline time.Time) (any, []byte, int, error) {
	var b backoff
	for {
		if l.closed {
			return nil, nil, 0, errors.New("use of closed network connection")
		}
		r := l.sock.Accept()
		if r.IsOk() {
			t := r.Ok()
			rip, rport := []byte(nil), 0
			if ra := t.F0.RemoteAddress(); ra.IsOk() {
				rip, rport = fromSocketAddress(ra.Ok())
			}
			return newConn(t.F0, t.F1, t.F2), rip, rport, nil
		}
		if r.Err() != wasi_sockets_network.ErrorCodeWouldBlock {
			return nil, nil, 0, sockErr("accept", r.Err())
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return nil, nil, 0, os.ErrDeadlineExceeded
		}
		b.wait()
	}
}

func (l *listener) Close() error {
	if !l.closed {
		l.closed = true
		l.sock.Drop()
	}
	return nil
}

type conn struct {
	sock          *wasi_sockets_tcp.TcpSocket
	in            *wasi_io_streams.InputStream
	out           *wasi_io_streams.OutputStream
	readDeadline  time.Time
	writeDeadline time.Time
	closed        bool
	readEOF       bool
}

func newConn(sock *wasi_sockets_tcp.TcpSocket, in *wasi_io_streams.InputStream, out *wasi_io_streams.OutputStream) *conn {
	return &conn{sock: sock, in: in, out: out}
}

var errClosed = errors.New("use of closed network connection")

func expired(d time.Time) bool { return !d.IsZero() && time.Now().After(d) }

func (c *conn) Read(p []byte) (int, error) {
	if c.closed {
		return 0, errClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if c.readEOF {
		return 0, io.EOF
	}
	var b backoff
	for {
		// Checked before every stream call: Close may have dropped the
		// handles while this goroutine slept.
		if c.closed {
			return 0, errClosed
		}
		if expired(c.readDeadline) {
			return 0, os.ErrDeadlineExceeded
		}
		r := c.in.Read(uint64(len(p)))
		if r.IsErr() {
			if r.Err().Tag() == wasi_io_streams.StreamErrorClosed {
				c.readEOF = true
				return 0, io.EOF
			}
			return 0, os.NewSyscallError("read", syscall.ECONNRESET)
		}
		if data := r.Ok(); len(data) > 0 {
			return copy(p, data), nil
		}
		b.wait()
	}
}

func (c *conn) Write(p []byte) (int, error) {
	written := 0
	var b backoff
	for written < len(p) {
		if c.closed {
			return written, errClosed
		}
		if expired(c.writeDeadline) {
			return written, os.ErrDeadlineExceeded
		}
		cw := c.out.CheckWrite()
		if cw.IsErr() {
			return written, os.NewSyscallError("write", syscall.EPIPE)
		}
		n := cw.Ok()
		if n == 0 {
			b.wait()
			continue
		}
		b = 0
		chunk := p[written:]
		if uint64(len(chunk)) > n {
			chunk = chunk[:n]
		}
		if r := c.out.Write(chunk); r.IsErr() {
			return written, os.NewSyscallError("write", syscall.EPIPE)
		}
		written += len(chunk)
	}
	// Start a flush; the next check-write reports 0 until it completes.
	c.out.Flush()
	return written, nil
}

// drain waits (cooperatively, at most a second) until buffered output has
// been flushed, so closing does not lose the tail of a write.
func (c *conn) drain() {
	c.out.Flush()
	deadline := time.Now().Add(time.Second)
	var b backoff
	for !c.closed && time.Now().Before(deadline) {
		cw := c.out.CheckWrite()
		if cw.IsErr() || cw.Ok() > 0 {
			return
		}
		b.wait()
	}
}

func (c *conn) CloseWrite() error {
	c.drain()
	if r := c.sock.Shutdown(wasi_sockets_tcp.ShutdownTypeSend); r.IsErr() {
		return sockErr("shutdown", r.Err())
	}
	return nil
}

func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.drain()
	c.closed = true
	c.in.Drop()
	c.out.Drop()
	c.sock.Drop()
	return nil
}

func (c *conn) SetReadDeadline(t time.Time) error  { c.readDeadline = t; return nil }
func (c *conn) SetWriteDeadline(t time.Time) error { c.writeDeadline = t; return nil }
