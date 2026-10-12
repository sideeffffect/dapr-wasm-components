package streams

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type str = wrapperspb.StringValue

// upper echoes every request upper-cased, reusing one response message
// (like daprd's crypto handler) to check that Send copies.
func upper(stream grpc.BidiStreamingServer[str, str]) error {
	resp := &str{}
	for {
		req := &str{}
		if err := stream.RecvMsg(req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		resp.Value = strings.ToUpper(req.GetValue())
		if err := stream.SendMsg(resp); err != nil {
			return err
		}
	}
}

func TestEchoUntilCloseSend(t *testing.T) {
	b := Start(context.Background(), upper)
	for _, v := range []string{"a", "b", "c"} {
		if err := b.Push(wrapperspb.String(v)); err != nil {
			t.Fatalf("push %q: %v", v, err)
		}
	}
	b.CloseSend()
	resps, err := b.Wait()
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	var got []string
	for _, r := range resps {
		got = append(got, r.GetValue())
	}
	if strings.Join(got, ",") != "A,B,C" {
		t.Fatalf("got %v", got)
	}
}

func TestPushAfterHandlerErrorReturnsIt(t *testing.T) {
	boom := errors.New("boom")
	b := Start(context.Background(), func(stream grpc.BidiStreamingServer[str, str]) error {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return boom
	})
	if err := b.Push(wrapperspb.String("first")); err != nil {
		t.Fatalf("first push: %v", err)
	}
	// The handler is gone; Push must not block.
	if err := b.Push(wrapperspb.String("second")); !errors.Is(err, boom) {
		t.Fatalf("second push: got %v, want %v", err, boom)
	}
	if _, err := b.Wait(); !errors.Is(err, boom) {
		t.Fatalf("wait: got %v", err)
	}
}

func TestPushAfterHandlerSuccess(t *testing.T) {
	b := Start(context.Background(), func(grpc.BidiStreamingServer[str, str]) error { return nil })
	if _, err := b.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := b.Push(wrapperspb.String("x")); !errors.Is(err, ErrHandlerDone) {
		t.Fatalf("got %v", err)
	}
}

func TestCancelUnblocksRecv(t *testing.T) {
	b := Start(context.Background(), upper)
	b.Cancel()
	if _, err := b.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if b.Context().Err() == nil {
		t.Fatal("context not cancelled")
	}
}
