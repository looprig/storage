package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
	"github.com/looprig/storage/storetest"
)

// plainBlobs hides every optional capability of the provider it embeds, so
// memstore's native BlobReaderLifecycle cannot short-circuit the adapter and the
// suites below exercise the pipe-backed wrapper itself.
type plainBlobs struct {
	storage.Blobs
}

func newPlainMemBlobs() storage.Blobs {
	return plainBlobs{Blobs: memstore.New().Blobs}
}

func mustBound(t *testing.T, b storage.Blobs) storage.BlobReaderLifecycle {
	t.Helper()
	bounded, err := storage.WithBoundedBlobReaders(b)
	if err != nil {
		t.Fatalf("WithBoundedBlobReaders: %v", err)
	}
	if _, native := b.(storage.BlobReaderLifecycle); !native && bounded == b {
		t.Fatal("WithBoundedBlobReaders returned the unwrapped provider")
	}
	return bounded
}

// The adapter must remain a conforming Blobs provider: wrapping may not change
// Put/Get/Delete/List semantics, nested-name coexistence included.
func TestBoundedBlobReadersBlobsConformance(t *testing.T) {
	t.Parallel()
	storetest.TestBlobs(t, func(t *testing.T) storage.Blobs { return mustBound(t, newPlainMemBlobs()) })
}

// The shared lifecycle suite exercises concurrency but, per its own doc, cannot
// create a genuinely blocked provider Read; the blocked-read test below does.
func TestBoundedBlobReadersLifecycleConformance(t *testing.T) {
	t.Parallel()
	storetest.TestBlobReaderLifecycle(t, func(t *testing.T) storage.BlobReaderLifecycle {
		return mustBound(t, newPlainMemBlobs())
	})
}

// blockingBlobs stands in for the uncancellable filesystem read (a wedged NFS
// or FUSE mount) the adapter exists for: its reader serves one byte, then parks
// in Read until release is closed.
type blockingBlobs struct {
	storage.Blobs
	release  chan struct{}
	closed   chan struct{}
	readDone chan struct{}
}

func (b *blockingBlobs) Get(context.Context, string) (io.ReadCloser, error) {
	return &blockingReader{b: b}, nil
}

type blockingReader struct {
	b      *blockingBlobs
	served bool
}

func (r *blockingReader) Read(p []byte) (int, error) {
	if !r.served {
		r.served = true
		return copy(p, "x"), nil
	}
	<-r.b.release
	close(r.b.readDone)
	return 0, io.EOF
}

func (r *blockingReader) Close() error {
	close(r.b.closed)
	return nil
}

// The load-bearing property: with a provider Read genuinely stuck, Close returns
// within the declared bound, a Read blocked on the stream terminates with a
// non-EOF *BlobReaderClosedError, and the abandoned provider reader is released
// once its Read finally returns.
func TestBoundedBlobReadersCloseIsBoundedWhileProviderReadIsBlocked(t *testing.T) {
	t.Parallel()
	provider := &blockingBlobs{
		Blobs:    newPlainMemBlobs(),
		release:  make(chan struct{}),
		closed:   make(chan struct{}),
		readDone: make(chan struct{}),
	}
	bounded := mustBound(t, provider)
	rc, err := bounded.Get(context.Background(), "blobs/stuck")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Drain the served byte so the pump is parked inside the provider's second Read.
	if n, err := rc.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatalf("first Read = (%d, %v), want (1, nil)", n, err)
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := rc.Read(make([]byte, 8))
		readErr <- err
	}()

	const watchdog = 5 * time.Second
	closed := make(chan error, 1)
	start := time.Now()
	go func() { closed <- rc.Close() }()
	select {
	case err := <-closed:
		if elapsed := time.Since(start); elapsed > bounded.BlobReaderCloseBound() {
			t.Errorf("Close took %v, exceeding the declared bound %v", elapsed, bounded.BlobReaderCloseBound())
		}
		if err != nil {
			t.Errorf("Close = %v, want nil", err)
		}
	case <-time.After(watchdog):
		t.Fatal("Close did not return while a provider Read was blocked")
	}

	select {
	case err := <-readErr:
		var closedErr *storage.BlobReaderClosedError
		if !errors.As(err, &closedErr) || errors.Is(err, io.EOF) || !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("blocked Read error = %v, want *BlobReaderClosedError wrapping io.ErrClosedPipe", err)
		}
	case <-time.After(watchdog):
		t.Fatal("blocked Read did not terminate after Close")
	}

	// Abandoned, not waited on: the provider reader is still open while its Read is stuck.
	select {
	case <-provider.closed:
		t.Fatal("provider reader was closed while its Read was still blocked")
	default:
	}
	close(provider.release)
	for _, done := range []chan struct{}{provider.readDone, provider.closed} {
		select {
		case <-done:
		case <-time.After(watchdog):
			t.Fatal("abandoned pump did not release the provider reader after its Read returned")
		}
	}
}

// scriptedBlobs returns a reader that serves prefix, then ends with readErr
// (io.EOF for a complete stream), and whose Close returns closeErr.
type scriptedBlobs struct {
	storage.Blobs
	prefix   string
	readErr  error
	closeErr error
	closes   *atomic.Int32
}

func (b scriptedBlobs) Get(context.Context, string) (io.ReadCloser, error) {
	return &scriptedReader{b: b}, nil
}

type scriptedReader struct {
	b    scriptedBlobs
	sent bool
}

func (r *scriptedReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, r.b.prefix), nil
	}
	return 0, r.b.readErr
}

func (r *scriptedReader) Close() error {
	if r.b.closes != nil {
		r.b.closes.Add(1)
	}
	return r.b.closeErr
}

// A stream must end in io.EOF only when it truly completed; every failure the
// provider reports — mid-stream or on release — reaches the caller as an error,
// after the bytes that preceded it, and the provider reader is always closed.
func TestBoundedBlobReadersStreamOutcome(t *testing.T) {
	t.Parallel()
	readFailed := errors.New("provider read failed")
	releaseFailed := errors.New("provider close failed")
	cases := []struct {
		name     string
		readErr  error
		closeErr error
		wantErr  error // nil means ReadAll must succeed (a genuine io.EOF)
	}{
		{name: "complete stream ends in EOF", readErr: io.EOF},
		{name: "mid-stream read error propagates", readErr: readFailed, wantErr: readFailed},
		{name: "mid-stream read error wins over close error", readErr: readFailed, closeErr: releaseFailed, wantErr: readFailed},
		{name: "close error after full read replaces EOF", readErr: io.EOF, closeErr: releaseFailed, wantErr: releaseFailed},
		{name: "provider ErrClosedPipe is not relabelled as caller close", readErr: io.ErrClosedPipe, wantErr: io.ErrClosedPipe},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var closes atomic.Int32
			bounded := mustBound(t, scriptedBlobs{prefix: "partial blob", readErr: tc.readErr, closeErr: tc.closeErr, closes: &closes})
			rc, err := bounded.Get(context.Background(), "blobs/scripted")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			data, err := io.ReadAll(rc)
			if string(data) != "partial blob" {
				t.Errorf("ReadAll data = %q, want the bytes before the outcome", data)
			}
			if tc.wantErr == nil && err != nil {
				t.Errorf("ReadAll error = %v, want nil (genuine EOF)", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("ReadAll error = %v, want %v", err, tc.wantErr)
			}
			var closedErr *storage.BlobReaderClosedError
			if errors.As(err, &closedErr) {
				t.Errorf("ReadAll error = %v, want the provider's error, not the caller-close error", err)
			}
			if err := rc.Close(); err != nil {
				t.Errorf("Close after drain = %v, want nil", err)
			}
			if got := closes.Load(); got != 1 {
				t.Errorf("provider reader closed %d times, want exactly once", got)
			}
		})
	}
}

// A full round trip through a real provider returns every byte and a genuine EOF.
func TestBoundedBlobReadersRoundTrip(t *testing.T) {
	t.Parallel()
	bounded := mustBound(t, newPlainMemBlobs())
	payload := bytes.Repeat([]byte("bounded blob payload "), 50_000)
	ctx := context.Background()
	if err := bounded.Put(ctx, "blobs/roundtrip", bytes.NewReader(payload)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := bounded.Get(ctx, "blobs/roundtrip")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round trip returned %d bytes, want %d", len(got), len(payload))
	}
}

type getResultBlobs struct {
	storage.Blobs
	rc  io.ReadCloser
	err error
}

func (b getResultBlobs) Get(context.Context, string) (io.ReadCloser, error) { return b.rc, b.err }

// Get failures are reported by Get itself, never deferred into the first Read.
func TestBoundedBlobReadersGetErrorsAreEager(t *testing.T) {
	t.Parallel()
	getFailed := errors.New("provider get failed")
	cases := []struct {
		name  string
		inner storage.Blobs
		check func(t *testing.T, err error)
	}{
		{
			name:  "provider error",
			inner: getResultBlobs{err: getFailed},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, getFailed) {
					t.Errorf("Get error = %v, want %v", err, getFailed)
				}
			},
		},
		{
			name:  "absent key keeps its typed error",
			inner: newPlainMemBlobs(),
			check: func(t *testing.T, err error) {
				var notFound *storage.BlobNotFoundError
				if !errors.As(err, &notFound) || notFound.Key != "blobs/missing" {
					t.Errorf("Get error = %v, want *BlobNotFoundError for blobs/missing", err)
				}
			},
		},
		{
			name:  "nil reader with nil error is refused",
			inner: getResultBlobs{},
			check: func(t *testing.T, err error) {
				var nilReader *storage.NilBlobReaderError
				if !errors.As(err, &nilReader) || nilReader.Key != "blobs/missing" {
					t.Errorf("Get error = %v, want *NilBlobReaderError for blobs/missing", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rc, err := mustBound(t, tc.inner).Get(context.Background(), "blobs/missing")
			if rc != nil {
				t.Errorf("Get reader = %v, want nil", rc)
			}
			tc.check(t, err)
		})
	}
}

func TestBoundedBlobReadersDeclaresOneSecondBound(t *testing.T) {
	t.Parallel()
	if storage.BoundedBlobReaderCloseBound != time.Second {
		t.Fatalf("BoundedBlobReaderCloseBound = %v, want 1s", storage.BoundedBlobReaderCloseBound)
	}
	if got := mustBound(t, newPlainMemBlobs()).BlobReaderCloseBound(); got != time.Second {
		t.Fatalf("BlobReaderCloseBound = %v, want 1s", got)
	}
}

// A provider that already conforms is returned unchanged, keeping its own bound
// and its genuine cancellation rather than acquiring a copy and abandonment.
func TestBoundedBlobReadersPassesThroughNativeLifecycle(t *testing.T) {
	t.Parallel()
	native := memstore.New().Blobs
	got, err := storage.WithBoundedBlobReaders(native)
	if err != nil {
		t.Fatalf("WithBoundedBlobReaders: %v", err)
	}
	if got != native {
		t.Errorf("WithBoundedBlobReaders(native) = %T, want the provider itself", got)
	}
	wrapped := mustBound(t, newPlainMemBlobs())
	if again, err := storage.WithBoundedBlobReaders(wrapped); err != nil || again != wrapped {
		t.Errorf("re-wrapping = (%T, %v), want the same adapter", again, err)
	}
}

func TestBoundedBlobReadersRefusesNilProvider(t *testing.T) {
	t.Parallel()
	got, err := storage.WithBoundedBlobReaders(nil)
	var nilBlobs *storage.NilBlobsError
	if got != nil || !errors.As(err, &nilBlobs) {
		t.Fatalf("WithBoundedBlobReaders(nil) = (%v, %v), want (nil, *NilBlobsError)", got, err)
	}
}

type pathBlobs struct {
	storage.Blobs
	paths []string
}

func (b pathBlobs) StoragePaths() []string { return slices.Clone(b.paths) }

// The adapter reports local roots exactly when the wrapped provider does.
func TestBoundedBlobReadersPathReporter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		inner storage.Blobs
		want  []string // nil means the adapter must not implement PathReporter
	}{
		{name: "forwarded", inner: pathBlobs{Blobs: newPlainMemBlobs(), paths: []string{"/data/blobs"}}, want: []string{"/data/blobs"}},
		{name: "not invented", inner: newPlainMemBlobs()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reporter, ok := mustBound(t, tc.inner).(storage.PathReporter)
			if tc.want == nil {
				if ok {
					t.Fatal("adapter implements PathReporter for a provider that does not")
				}
				return
			}
			if !ok {
				t.Fatal("adapter dropped the provider's PathReporter")
			}
			if got := reporter.StoragePaths(); !slices.Equal(got, tc.want) {
				t.Errorf("StoragePaths = %v, want %v", got, tc.want)
			}
		})
	}
}

// The Composite helper returns a copy with only Blobs adapted, and never
// mutates the composite it was called on.
func TestCompositeWithBoundedBlobReaders(t *testing.T) {
	t.Parallel()
	base := memstore.New()
	plain := newPlainMemBlobs()
	in := &storage.Composite{Ledger: base.Ledger, Leaser: base.Leaser, KV: base.KV, Blobs: plain, OrderedIndex: base.OrderedIndex}
	before := *in

	out, err := in.WithBoundedBlobReaders()
	if err != nil {
		t.Fatalf("WithBoundedBlobReaders: %v", err)
	}
	if out == in {
		t.Fatal("WithBoundedBlobReaders returned the input composite, want a copy")
	}
	if *in != before {
		t.Error("WithBoundedBlobReaders mutated its input composite")
	}
	if _, ok := out.Blobs.(storage.BlobReaderLifecycle); !ok {
		t.Error("output Blobs does not implement BlobReaderLifecycle")
	}
	if out.Ledger != in.Ledger || out.Leaser != in.Leaser || out.KV != in.KV || out.OrderedIndex != in.OrderedIndex {
		t.Error("WithBoundedBlobReaders changed a primitive other than Blobs")
	}
}

func TestCompositeWithBoundedBlobReadersRefusesMissingBlobs(t *testing.T) {
	t.Parallel()
	base := memstore.New()
	cases := []struct {
		name string
		in   *storage.Composite
	}{
		{name: "nil composite"},
		{name: "nil Blobs", in: &storage.Composite{Ledger: base.Ledger, Leaser: base.Leaser, KV: base.KV}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out, err := tc.in.WithBoundedBlobReaders()
			var incomplete *storage.IncompleteCompositeError
			if out != nil || !errors.As(err, &incomplete) || !slices.Equal(incomplete.Missing, []string{"Blobs"}) {
				t.Fatalf("WithBoundedBlobReaders = (%v, %v), want (nil, missing [Blobs])", out, err)
			}
		})
	}
}

// A local, single-host deployment opts its filesystem-shaped Blobs provider into
// bounded readers before handing the composite to a consumer that requires
// storage.BlobReaderLifecycle. (plainBlobs stands in for such a provider here.)
func ExampleComposite_WithBoundedBlobReaders() {
	memory := memstore.New()
	local := &storage.Composite{
		Ledger: memory.Ledger, Leaser: memory.Leaser, KV: memory.KV,
		Blobs: plainBlobs{Blobs: memory.Blobs}, OrderedIndex: memory.OrderedIndex,
	}
	backend, err := local.WithBoundedBlobReaders()
	if err != nil {
		panic(err)
	}
	lifecycle, ok := backend.Blobs.(storage.BlobReaderLifecycle)
	fmt.Println(ok, lifecycle.BlobReaderCloseBound())
	// Output: true 1s
}
