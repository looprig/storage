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
		result := make(chan timedResult[error], 1)
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
					result <- timedResult[error]{value: errReadSucceededAfterClose, finishedAt: time.Now()}
					return
				}
				if readErr != nil {
					result <- timedResult[error]{value: readErr, finishedAt: time.Now()}
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
		case readResult := <-result:
			t.Fatalf("read loop terminated before Close: %v", readResult.value)
		default:
		}
		firstCloseCall := beginClose(rc, &closeReturned)
		closeStarted := receiveBeforeContext(t, ctx, firstCloseCall.started, "first Close invocation")
		close(resume)
		limit := lifecycleLimit(ctx, closeStarted, b.BlobReaderCloseBound())
		firstClose := receiveBefore(t, ctx, firstCloseCall.result, limit, "first Close")
		readErr := receiveBefore(t, ctx, result, limit, "active Read")
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
	result  <-chan timedResult[T]
}

type timedResult[T any] struct {
	value      T
	finishedAt time.Time
}

type timedLimit struct {
	deadline time.Time
	exceeded waitOutcome
}

type waitOutcome uint8

const (
	waitSucceeded waitOutcome = iota
	waitBoundExceeded
	waitContextEnded
)

func beginTimedCall[T any](call func() T) timedCall[T] {
	started := make(chan time.Time, 1)
	result := make(chan timedResult[T], 1)
	go func() {
		// This is the closest observable point before invocation. A goroutine can
		// still be descheduled between this timestamp and call(); that scheduling
		// delay is conservatively charged to the provider's advertised bound.
		started <- time.Now()
		value := call()
		result <- timedResult[T]{value: value, finishedAt: time.Now()}
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
	return receiveBefore(t, ctx, call.result, lifecycleLimit(ctx, started, bound), "Close")
}

func lifecycleLimit(ctx context.Context, started time.Time, bound time.Duration) timedLimit {
	deadline := started.Add(bound)
	if contextDeadline, ok := ctx.Deadline(); ok && !contextDeadline.After(deadline) {
		return timedLimit{deadline: contextDeadline, exceeded: waitContextEnded}
	}
	return timedLimit{deadline: deadline, exceeded: waitBoundExceeded}
}

// lifecycleWatchdog is the finite exception to the shared conformance context
// being the final observation cutoff. Lifecycle Read and Close do not accept a
// context; they are measured against the earlier of the advertised provider
// bound and shared context deadline. Results may be published after that
// effective deadline and are judged by their captured completion timestamp. A
// missing publication gets one full conformance timeout beyond the deadline
// before the suite declares a stall. Publication after the watchdog is
// intentionally a stall even if its captured timestamp claims an earlier
// completion; no finite observer can distinguish that from a goroutine that will
// never publish.
func lifecycleWatchdog(ctx context.Context, limit timedLimit) (context.Context, context.CancelFunc) {
	return context.WithDeadline(context.WithoutCancel(ctx), limit.deadline.Add(conformanceTimeout))
}

func receiveBeforeContext(t *testing.T, ctx context.Context, result <-chan time.Time, operation string) time.Time {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("%s has no conformance deadline", operation)
	}
	limit := timedLimit{deadline: deadline, exceeded: waitContextEnded}
	watchdog, cancel := lifecycleWatchdog(ctx, limit)
	defer cancel()
	started, outcome := waitStart(watchdog, result, limit)
	if outcome == waitSucceeded {
		return started
	}
	t.Fatalf("%s did not occur within the conformance timeout", operation)
	return time.Time{}
}

func waitStart(watchdog context.Context, result <-chan time.Time, limit timedLimit) (time.Time, waitOutcome) {
	classify := func(started time.Time) (time.Time, waitOutcome) {
		if started.After(limit.deadline) {
			return started, limit.exceeded
		}
		return started, waitSucceeded
	}
	ready := func() (time.Time, waitOutcome, bool) {
		select {
		case started := <-result:
			classified, outcome := classify(started)
			return classified, outcome, true
		default:
			return time.Time{}, waitSucceeded, false
		}
	}
	if started, outcome, received := ready(); received {
		return started, outcome
	}
	select {
	case started := <-result:
		return classify(started)
	case <-watchdog.Done():
		if started, outcome, received := ready(); received {
			return started, outcome
		}
		return time.Time{}, limit.exceeded
	}
}

func receiveBefore[T any](t *testing.T, ctx context.Context, result <-chan timedResult[T], limit timedLimit, operation string) T {
	t.Helper()
	watchdog, cancel := lifecycleWatchdog(ctx, limit)
	defer cancel()
	timed, outcome := waitTimedResult(watchdog, result, limit)
	switch outcome {
	case waitSucceeded:
		return timed.value
	case waitContextEnded:
		t.Fatalf("%s did not return within the conformance timeout", operation)
	case waitBoundExceeded:
		t.Fatalf("%s did not return within the bounded lifecycle wait", operation)
	}
	var zero T
	return zero
}

func waitTimedResult[T any](watchdog context.Context, result <-chan timedResult[T], limit timedLimit) (timedResult[T], waitOutcome) {
	classify := func(timed timedResult[T]) (timedResult[T], waitOutcome) {
		if timed.finishedAt.After(limit.deadline) {
			return timed, limit.exceeded
		}
		return timed, waitSucceeded
	}
	ready := func() (timedResult[T], waitOutcome, bool) {
		select {
		case timed := <-result:
			classified, outcome := classify(timed)
			return classified, outcome, true
		default:
			var zero timedResult[T]
			return zero, waitSucceeded, false
		}
	}
	if timed, outcome, ok := ready(); ok {
		return timed, outcome
	}
	select {
	case timed := <-result:
		return classify(timed)
	case <-watchdog.Done():
		if timed, outcome, ok := ready(); ok {
			return timed, outcome
		}
		var zero timedResult[T]
		return zero, limit.exceeded
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
		result := receiveBefore(t, ctx, call.result, lifecycleLimit(ctx, started, bound), "post-Close Read")
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
