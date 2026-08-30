package storetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/storage"
)

var errReadSucceededAfterClose = errors.New("read succeeded after Close returned")

// TestBlobReaderLifecycle runs the optional bounded Blob reader lifecycle
// conformance suite. Providers that claim storage.BlobReaderLifecycle must also
// test genuinely blocked provider I/O with a backend-specific deterministic
// probe; this shared suite exercises concurrency but cannot create such a wait.
func TestBlobReaderLifecycle(t *testing.T, newBackend func(t *testing.T) storage.BlobReaderLifecycle) {
	ctx := conformanceContext(t)

	t.Run("bound is positive", func(t *testing.T) {
		if bound := newBackend(t).BlobReaderCloseBound(); bound <= 0 {
			t.Fatalf("BlobReaderCloseBound = %v, want positive duration", bound)
		}
	})

	t.Run("Close before first Read is bounded and terminal", func(t *testing.T) {
		b := newBackend(t)
		const key = "blobs/lifecycle-unread"
		if err := b.Put(ctx, key, bytes.NewReader([]byte("unread bytes"))); err != nil {
			t.Fatalf("Put: %v", err)
		}
		rc, err := b.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		requireConcreteReader(t, rc)
		firstClose := closeWithin(t, ctx, rc, b.BlobReaderCloseBound())
		secondClose := closeWithin(t, ctx, rc, b.BlobReaderCloseBound())
		requireStableCloseClassification(t, firstClose, secondClose)
		requirePostCloseReads(t, ctx, rc, b.BlobReaderCloseBound())
	})

	t.Run("Read loop and Close terminate within bound", func(t *testing.T) {
		b := newBackend(t)
		const key = "blobs/lifecycle-concurrent"
		if err := b.Put(ctx, key, bytes.NewReader(patternedBytes(payloadFloor))); err != nil {
			t.Fatalf("Put: %v", err)
		}
		rc, err := b.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		requireConcreteReader(t, rc)

		started := make(chan struct{})
		resume := make(chan struct{})
		result := make(chan error, 1)
		var closeReturned atomic.Bool
		go func() {
			first := true
			var one [1]byte
			for {
				beganAfterClose := closeReturned.Load()
				n, readErr := rc.Read(one[:])
				if first {
					first = false
					close(started)
					if readErr == nil {
						<-resume
					}
				}
				if beganAfterClose && n > 0 {
					result <- errReadSucceededAfterClose
					return
				}
				if readErr != nil {
					result <- readErr
					return
				}
			}
		}()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("initial Read did not return within the conformance timeout")
		}
		select {
		case readErr := <-result:
			t.Fatalf("read loop terminated before Close: %v", readErr)
		default:
		}
		firstCloseCall := beginClose(rc, &closeReturned)
		closeStarted := receiveBeforeContext(t, ctx, firstCloseCall.started, "first Close invocation")
		close(resume)
		deadline := lifecycleDeadline(ctx, closeStarted, b.BlobReaderCloseBound())
		firstClose := receiveBefore(t, ctx, firstCloseCall.result, deadline, "first Close")
		readErr := receiveBefore(t, ctx, result, deadline, "active Read")
		if errors.Is(readErr, errReadSucceededAfterClose) {
			t.Fatal(readErr)
		}
		if readErr == nil || errors.Is(readErr, io.EOF) {
			t.Fatalf("read loop terminal error = %v, want non-EOF error", readErr)
		}
		secondClose := closeWithin(t, ctx, rc, b.BlobReaderCloseBound())
		requireStableCloseClassification(t, firstClose, secondClose)
		requirePostCloseReads(t, ctx, rc, b.BlobReaderCloseBound())
	})
}

func requireConcreteReader(t *testing.T, rc io.ReadCloser) {
	t.Helper()
	if rc == nil {
		t.Fatal("Get returned nil reader with nil error")
	}
	v := reflect.ValueOf(rc)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			t.Fatalf("Get returned typed-nil %T reader with nil error", rc)
		}
	}
}

type timedCall[T any] struct {
	started <-chan time.Time
	result  <-chan T
}

func beginTimedCall[T any](call func() T) timedCall[T] {
	started := make(chan time.Time, 1)
	result := make(chan T, 1)
	go func() {
		started <- time.Now()
		result <- call()
	}()
	return timedCall[T]{started: started, result: result}
}

func beginClose(rc io.Closer, returned *atomic.Bool) timedCall[error] {
	return beginTimedCall(func() error {
		err := rc.Close()
		if returned != nil {
			returned.Store(true)
		}
		return err
	})
}

func closeWithin(t *testing.T, ctx context.Context, rc io.ReadCloser, bound time.Duration) error {
	t.Helper()
	call := beginClose(rc, nil)
	started := receiveBeforeContext(t, ctx, call.started, "Close invocation")
	return receiveBefore(t, ctx, call.result, lifecycleDeadline(ctx, started, bound), "Close")
}

func lifecycleDeadline(ctx context.Context, started time.Time, bound time.Duration) time.Time {
	deadline := started.Add(bound)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}

func receiveBeforeContext[T any](t *testing.T, ctx context.Context, result <-chan T, operation string) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		t.Fatalf("%s did not occur within the conformance timeout", operation)
		var zero T
		return zero
	}
}

func receiveBefore[T any](t *testing.T, ctx context.Context, result <-chan T, deadline time.Time, operation string) T {
	t.Helper()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatalf("%s did not return within the bounded lifecycle wait", operation)
		var zero T
		return zero
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case value := <-result:
		return value
	case <-ctx.Done():
		t.Fatalf("%s did not return within the conformance timeout", operation)
		var zero T
		return zero
	case <-timer.C:
		t.Fatalf("%s did not return within the bounded lifecycle wait", operation)
		var zero T
		return zero
	}
}

func requireStableCloseClassification(t *testing.T, first, second error) {
	t.Helper()
	if (first == nil) != (second == nil) || (first != nil && !errors.Is(first, second) && !errors.Is(second, first)) {
		t.Fatalf("Close results are not stable: first=%v second=%v", first, second)
	}
}

func requirePostCloseReads(t *testing.T, ctx context.Context, rc io.Reader, bound time.Duration) {
	t.Helper()
	for i := 0; i < 3; i++ {
		call := beginTimedCall(func() readResult {
			var p [8]byte
			n, err := rc.Read(p[:])
			return readResult{n: n, err: err}
		})
		started := receiveBeforeContext(t, ctx, call.started, "post-Close Read invocation")
		result := receiveBefore(t, ctx, call.result, lifecycleDeadline(ctx, started, bound), "post-Close Read")
		n, err := result.n, result.err
		if n != 0 || err == nil || errors.Is(err, io.EOF) {
			t.Fatalf("Read after Close call %d = %d, %v; want 0 and non-EOF error", i, n, err)
		}
	}
}

type readResult struct {
	n   int
	err error
}
