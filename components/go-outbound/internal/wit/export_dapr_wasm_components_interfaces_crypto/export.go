// Cryptography (alpha), served by the embedded runtime's API —
// EncryptAlpha1, DecryptAlpha1. Mirrors wasi-grpc-outbound's crypto.rs.
//
// Both methods are bidirectional streams. The request messages are the
// same as crypto.rs sends: options on the first, the payload split into
// chunk-sized StreamPayloads with seq 0, 1, …; the response chunks are
// reassembled in seq order.
//
// Unlike crypto.rs (which cannot keep a call alive across export calls and
// so buffers the whole input), the in-process stream stays open between
// calls: the encryptor/decryptor resources start the call on the first
// write and stream each write to the runtime as it comes. The output is
// still collected in memory and served by crypto-output.
package export_dapr_wasm_components_interfaces_crypto

import (
	"bytes"
	"runtime"
	"slices"
	"strings"

	commonv1pb "github.com/dapr/dapr/pkg/proto/common/v1"
	runtimev1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	witTypes "go.bytecodealliance.org/pkg/wit/types"
	"google.golang.org/grpc"

	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/call"
	"github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/streams"
	cr "github.com/sideeffffect/dapr-wasm-components/components/go-outbound/internal/wit/dapr_wasm_components_interfaces_crypto"
)

// chunk is the payload size per request message (well under daprd's 4Mi
// default per-message limit, as in crypto.rs).
const chunk = 1024 * 1024

func cryptoError(f *call.Failure) cr.CryptoError {
	if f.IsPermission() {
		return cr.MakeCryptoErrorPermissionDenied(f.Message)
	}
	return cr.MakeCryptoErrorComponentNotFound(f.Message)
}

// isProviderNotFound: daprd answers an unknown component with
// InvalidArgument and "crypto provider <name> not found".
func isProviderNotFound(f *call.Failure) bool {
	return strings.Contains(f.Message, "crypto provider") && strings.Contains(f.Message, "not found")
}

func isKeyNotFound(f *call.Failure) bool {
	return f.Status == 404 || strings.Contains(f.ErrorCode, "KEY_NOT_FOUND") || strings.Contains(f.ErrorCode, "NOT_FOUND")
}

func encryptError(err error) cr.EncryptError {
	f := call.Classify(err)
	switch {
	case isProviderNotFound(f):
		return cr.MakeEncryptErrorCrypto(cr.MakeCryptoErrorComponentNotFound(f.Message))
	case isKeyNotFound(f):
		return cr.MakeEncryptErrorKeyNotFound(f.Message)
	case f.Status == 400:
		return cr.MakeEncryptErrorUnsupportedAlgorithm(f.Message)
	}
	return cr.MakeEncryptErrorCrypto(cryptoError(f))
}

func decryptError(err error) cr.DecryptError {
	f := call.Classify(err)
	switch {
	case isProviderNotFound(f):
		return cr.MakeDecryptErrorCrypto(cr.MakeCryptoErrorComponentNotFound(f.Message))
	case isKeyNotFound(f):
		return cr.MakeDecryptErrorKeyNotFound(f.Message)
	case f.Status == 400:
		return cr.MakeDecryptErrorMalformedCiphertext(f.Message)
	}
	return cr.MakeDecryptErrorCrypto(cryptoError(f))
}

// session is one EncryptAlpha1/DecryptAlpha1 call, started lazily: daprd
// waits only 5s for the first message, so the call must not start before
// there is something to send.
type session[Req, Resp any] struct {
	handler func(grpc.BidiStreamingServer[Req, Resp]) error
	// request builds a request message; options go on the first one only.
	request func(first bool, payload *commonv1pb.StreamPayload) *Req
	payload func(*Resp) *commonv1pb.StreamPayload

	stream *streams.Bidi[Req, Resp]
	seq    uint64
	err    error // sticky: once the call failed, every later op reports it
}

// write sends data as chunk-sized payloads. data must stay unmodified until
// the call ends (the runtime may still be consuming it after write returns).
func (s *session[Req, Resp]) write(data []byte) error {
	if s.err != nil {
		return s.err
	}
	if s.stream == nil {
		s.stream = streams.Start(call.Context(), s.handler)
	}
	for start := 0; start == 0 || start < len(data); start += chunk {
		end := min(start+chunk, len(data))
		p := &commonv1pb.StreamPayload{Data: data[start:end], Seq: s.seq}
		if err := s.stream.Push(s.request(s.seq == 0, p)); err != nil {
			s.err = err
			return err
		}
		s.seq++
	}
	return nil
}

// finish half-closes the call and returns the output reassembled in seq
// order. With no input written yet it sends one empty payload, like
// crypto.rs does for empty input.
func (s *session[Req, Resp]) finish() ([]byte, error) {
	if s.seq == 0 {
		if err := s.write(nil); err != nil {
			return nil, err
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	s.stream.CloseSend()
	responses, err := s.stream.Wait()
	if err != nil {
		s.err = err
		return nil, err
	}
	chunks := make([]*commonv1pb.StreamPayload, 0, len(responses))
	for _, r := range responses {
		if p := s.payload(r); p != nil {
			chunks = append(chunks, p)
		}
	}
	// One stream already delivers them in order; sorting enforces the
	// proto contract.
	slices.SortStableFunc(chunks, func(a, b *commonv1pb.StreamPayload) int {
		switch {
		case a.GetSeq() < b.GetSeq():
			return -1
		case a.GetSeq() > b.GetSeq():
			return 1
		}
		return 0
	})
	var out bytes.Buffer
	for _, p := range chunks {
		out.Write(p.GetData())
	}
	return out.Bytes(), nil
}

// cancel aborts an unfinished call (the resource was dropped).
func (s *session[Req, Resp]) cancel() {
	if s.stream != nil {
		s.stream.Cancel()
	}
}

type encryptSession = session[runtimev1pb.EncryptRequest, runtimev1pb.EncryptResponse]
type decryptSession = session[runtimev1pb.DecryptRequest, runtimev1pb.DecryptResponse]

func newEncryptSession(componentName string, options cr.EncryptOptions) *encryptSession {
	opts := &runtimev1pb.EncryptRequestOptions{
		ComponentName:         componentName,
		KeyName:               options.KeyName,
		KeyWrapAlgorithm:      options.KeyWrapAlgorithm,
		DataEncryptionCipher:  call.OrEmpty(options.DataEncryptionCipher),
		OmitDecryptionKeyName: options.OmitDecryptionKeyName.SomeOr(false),
		DecryptionKeyName:     call.OrEmpty(options.DecryptionKeyName),
	}
	return &encryptSession{
		handler: func(stream grpc.BidiStreamingServer[runtimev1pb.EncryptRequest, runtimev1pb.EncryptResponse]) error {
			return call.API().EncryptAlpha1(stream)
		},
		request: func(first bool, p *commonv1pb.StreamPayload) *runtimev1pb.EncryptRequest {
			req := &runtimev1pb.EncryptRequest{Payload: p}
			if first {
				req.Options = opts
			}
			return req
		},
		payload: (*runtimev1pb.EncryptResponse).GetPayload,
	}
}

func newDecryptSession(componentName string, keyName witTypes.Option[string]) *decryptSession {
	opts := &runtimev1pb.DecryptRequestOptions{ComponentName: componentName, KeyName: call.OrEmpty(keyName)}
	return &decryptSession{
		handler: func(stream grpc.BidiStreamingServer[runtimev1pb.DecryptRequest, runtimev1pb.DecryptResponse]) error {
			return call.API().DecryptAlpha1(stream)
		},
		request: func(first bool, p *commonv1pb.StreamPayload) *runtimev1pb.DecryptRequest {
			req := &runtimev1pb.DecryptRequest{Payload: p}
			if first {
				req.Options = opts
			}
			return req
		},
		payload: (*runtimev1pb.DecryptResponse).GetPayload,
	}
}

// oneShot runs a whole call over in-memory input.
func oneShot[Req, Resp any](s *session[Req, Resp], data []byte) ([]byte, error) {
	if len(data) > 0 {
		if err := s.write(data); err != nil {
			return nil, err
		}
	}
	return s.finish()
}

func Encrypt(componentName string, data []uint8, options cr.EncryptOptions) witTypes.Result[[]uint8, cr.EncryptError] {
	out, err := oneShot(newEncryptSession(componentName, options), data)
	if err != nil {
		return witTypes.Err[[]uint8](encryptError(err))
	}
	return witTypes.Ok[[]uint8, cr.EncryptError](out)
}

func Decrypt(componentName string, data []uint8, keyName witTypes.Option[string]) witTypes.Result[[]uint8, cr.DecryptError] {
	out, err := oneShot(newDecryptSession(componentName, keyName), data)
	if err != nil {
		return witTypes.Err[[]uint8](decryptError(err))
	}
	return witTypes.Ok[[]uint8, cr.DecryptError](out)
}

// Encryptor streams each write into one EncryptAlpha1 call.
type Encryptor struct {
	pinner  runtime.Pinner
	handle  int32
	session *encryptSession
}

// Decryptor streams each write into one DecryptAlpha1 call.
type Decryptor struct {
	pinner  runtime.Pinner
	handle  int32
	session *decryptSession
}

// CryptoOutput serves a finished call's output in chunks.
type CryptoOutput struct {
	pinner   runtime.Pinner
	handle   int32
	data     []byte
	position int
}

func MakeEncryptor(componentName string, options cr.EncryptOptions) *Encryptor {
	return &Encryptor{session: newEncryptSession(componentName, options)}
}

// Write copies chunk: the argument buffer is not ours past this call, but
// the runtime may still be reading it.
func (self *Encryptor) Write(chunk []uint8) witTypes.Result[witTypes.Unit, cr.EncryptError] {
	if self.session == nil {
		panic("encryptor used after finish")
	}
	if len(chunk) > 0 {
		if err := self.session.write(bytes.Clone(chunk)); err != nil {
			return witTypes.Err[witTypes.Unit](encryptError(err))
		}
	}
	return witTypes.Ok[witTypes.Unit, cr.EncryptError](witTypes.Unit{})
}

// EncryptorFinish consumes this: the own handle passed in is dropped (its
// destructor runs OnDrop, after the session has been taken out).
func EncryptorFinish(this *Encryptor) witTypes.Result[*CryptoOutput, cr.EncryptError] {
	s := this.session
	this.session = nil
	this.Drop()
	out, err := s.finish()
	if err != nil {
		return witTypes.Err[*CryptoOutput](encryptError(err))
	}
	return witTypes.Ok[*CryptoOutput, cr.EncryptError](&CryptoOutput{data: out})
}

func MakeDecryptor(componentName string, keyName witTypes.Option[string]) *Decryptor {
	return &Decryptor{session: newDecryptSession(componentName, keyName)}
}

// Write copies chunk, see Encryptor.Write.
func (self *Decryptor) Write(chunk []uint8) witTypes.Result[witTypes.Unit, cr.DecryptError] {
	if self.session == nil {
		panic("decryptor used after finish")
	}
	if len(chunk) > 0 {
		if err := self.session.write(bytes.Clone(chunk)); err != nil {
			return witTypes.Err[witTypes.Unit](decryptError(err))
		}
	}
	return witTypes.Ok[witTypes.Unit, cr.DecryptError](witTypes.Unit{})
}

// DecryptorFinish consumes this, see EncryptorFinish.
func DecryptorFinish(this *Decryptor) witTypes.Result[*CryptoOutput, cr.DecryptError] {
	s := this.session
	this.session = nil
	this.Drop()
	out, err := s.finish()
	if err != nil {
		return witTypes.Err[*CryptoOutput](decryptError(err))
	}
	return witTypes.Ok[*CryptoOutput, cr.DecryptError](&CryptoOutput{data: out})
}

// Read returns up to maxLen bytes (at least 1, like crypto.rs), none at
// the end.
func (self *CryptoOutput) Read(maxLen uint64) witTypes.Option[[]uint8] {
	remaining := len(self.data) - self.position
	if remaining <= 0 {
		return witTypes.None[[]uint8]()
	}
	n := remaining
	if maxLen < uint64(remaining) {
		n = int(max(maxLen, 1))
	}
	start := self.position
	self.position += n
	return witTypes.Some(self.data[start:self.position])
}

// OnDrop may run twice (generated Drop and the dtor), so it is idempotent.
func (self *Encryptor) OnDrop() {
	if self.session != nil {
		self.session.cancel()
		self.session = nil
	}
}

func (self *Decryptor) OnDrop() {
	if self.session != nil {
		self.session.cancel()
		self.session = nil
	}
}

func (self *CryptoOutput) OnDrop() { self.data = nil }
