package storage

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// BoundedBlobReaderCloseBound is the BlobReaderCloseBound published by a
// provider adapted with WithBoundedBlobReaders. It is a declared ceiling, not an
// estimate: the adapter's Close is a pipe close plus a latch — microseconds and
// no I/O — and it never waits on the wrapped provider, so a full second is a
// deliberately generous bound that always holds rather than a tight one.
const BoundedBlobReaderCloseBound = time.Second

// WithBoundedBlobReaders is an EXPLICIT OPT-IN that adapts a Blobs provider
// which does not implement BlobReaderLifecycle — typically a local filesystem
// store — so that it does. It exists so a single-host deployment can back a
// consumer that requires bounded reader shutdown (for example sessionstore)
// with a provider that, on its own, correctly refuses to promise one.
//
// # How it bounds Close
//
// Get opens the provider's reader eagerly, then hands the caller the read half
// of an in-memory pipe and copies the provider reader into the write half on a
// goroutine the adapter owns (the pump). The returned reader's Close closes the
// pipe: it returns within BoundedBlobReaderCloseBound, and any Read blocked on
// the pipe at that moment — or attempted afterwards — returns zero bytes and a
// *BlobReaderClosedError. Close never waits for the provider.
//
// # What it costs: abandonment
//
// "Bounded" describes the caller's Close and the caller's Read only. The
// provider's own Read is NOT cancelled — portable file I/O cannot be — it is
// abandoned. If the caller closes while the pump is parked inside a provider
// Read, the pump goroutine, the provider reader and whatever it holds (for a
// filesystem store, an open file descriptor) stay alive until that provider
// Read returns on its own; only then does the pump observe the closed pipe,
// Close the provider reader and exit. On a local disk that is microseconds. On a
// wedged network or FUSE mount it can be forever: the adapter converts an
// unbounded Close into a bounded Close plus a possibly unbounded leak of one
// goroutine and one provider reader per abandoned stream. That trade is sound
// for a local directory the process owns, and is why this adapter is for
// LOCAL, SINGLE-HOST use. A distributed or network backend should implement
// BlobReaderLifecycle natively, by genuinely cancelling its I/O (as s3store
// does), instead of being wrapped.
//
// A caller must still Close every reader it obtains, exactly as for any
// io.ReadCloser: a reader that is neither drained nor closed leaves its pump
// parked on the pipe indefinitely.
//
// # Stream integrity
//
//   - A stream read to its end ends in a genuine io.EOF, after the provider
//     reader has been closed successfully.
//   - A provider Read error mid-stream reaches the caller's Read as that error
//     (errors.Is holds), after any bytes delivered before it. It is never turned
//     into a clean io.EOF, so a truncated stream cannot pass for a complete one.
//   - A provider reader Close error after a complete read is likewise delivered
//     in place of io.EOF: the stream is reported failed, not complete.
//   - A provider Get error is returned by Get itself, eagerly, never deferred
//     into the first Read. A provider that returns a nil reader with a nil error
//     is refused with *NilBlobReaderError rather than crashing the pump.
//
// The stream is otherwise bounded only by the provider's own handling of the
// Get context; the adapter adds no deadline of its own. The pipe adds one copy
// per chunk.
//
// # Pass-through and capabilities
//
// A provider that already implements BlobReaderLifecycle is returned unchanged,
// with its own bound: wrapping it would add a copy and replace a genuine
// cancellation with abandonment. Put, Delete and List delegate unchanged. The
// adapter implements PathReporter exactly when the wrapped provider does, so
// wrapping never hides a provider's local roots nor invents any.
//
// A nil provider is refused with *NilBlobsError rather than wrapped, so a
// consumer's own "missing Blobs" diagnosis is not replaced by a nil
// dereference at first use.
func WithBoundedBlobReaders(b Blobs) (BlobReaderLifecycle, error) {
	if b == nil {
		return nil, &NilBlobsError{}
	}
	if lifecycle, ok := b.(BlobReaderLifecycle); ok {
		return lifecycle, nil
	}
	bounded := &boundedBlobs{inner: b}
	if reporter, ok := b.(PathReporter); ok {
		return &boundedPathBlobs{boundedBlobs: bounded, reporter: reporter}, nil
	}
	return bounded, nil
}

// WithBoundedBlobReaders returns a copy of c whose Blobs field is adapted by the
// package-level WithBoundedBlobReaders; every other field is carried over
// unchanged. c itself is never mutated, so a composite shared with a consumer
// that does not want the adapter keeps its original provider. A nil composite or
// one with no Blobs provider is reported as *IncompleteCompositeError naming
// Blobs, the same diagnosis NewComposite gives.
func (c *Composite) WithBoundedBlobReaders() (*Composite, error) {
	if c == nil || c.Blobs == nil {
		return nil, &IncompleteCompositeError{Missing: []string{"Blobs"}}
	}
	bounded, err := WithBoundedBlobReaders(c.Blobs)
	if err != nil {
		return nil, err
	}
	out := *c
	out.Blobs = bounded
	return &out, nil
}

// NilBlobsError reports that WithBoundedBlobReaders was handed a nil Blobs
// provider.
type NilBlobsError struct{}

func (e *NilBlobsError) Error() string {
	return "storage: bounded blob readers: nil Blobs provider"
}

// NilBlobReaderError reports that a wrapped provider's Get returned a nil reader
// with a nil error, which the Blobs contract does not permit.
type NilBlobReaderError struct {
	Key string
}

func (e *NilBlobReaderError) Error() string {
	return "storage: blob " + strconv.Quote(e.Key) + " provider returned a nil reader"
}

// BlobReaderClosedError is the terminal error a bounded blob reader's Read
// returns once its Close has begun. It is deliberately not io.EOF, so a stream
// cut short by Close can never be mistaken for a complete one. It wraps
// io.ErrClosedPipe, the condition the reader actually observed.
type BlobReaderClosedError struct{}

func (e *BlobReaderClosedError) Error() string {
	return "storage: bounded blob reader closed"
}

// Unwrap returns io.ErrClosedPipe.
func (e *BlobReaderClosedError) Unwrap() error { return io.ErrClosedPipe }

var (
	_ BlobReaderLifecycle = (*boundedBlobs)(nil)
	_ BlobReaderLifecycle = (*boundedPathBlobs)(nil)
	_ PathReporter        = (*boundedPathBlobs)(nil)
)

// boundedBlobs is the adapter for a provider that reports no local paths.
type boundedBlobs struct {
	inner Blobs
}

// boundedPathBlobs is the adapter for a provider that implements PathReporter;
// it forwards StoragePaths so wrapping does not narrow the provider.
type boundedPathBlobs struct {
	*boundedBlobs
	reporter PathReporter
}

func (b *boundedPathBlobs) StoragePaths() []string { return b.reporter.StoragePaths() }

func (b *boundedBlobs) Put(ctx context.Context, key string, r io.Reader) error {
	return b.inner.Put(ctx, key, r)
}

func (b *boundedBlobs) Delete(ctx context.Context, key string) error {
	return b.inner.Delete(ctx, key)
}

func (b *boundedBlobs) List(ctx context.Context, prefix string) ([]string, error) {
	return b.inner.List(ctx, prefix)
}

// BlobReaderCloseBound reports BoundedBlobReaderCloseBound. It is a property of
// the pipe handoff, not of the wrapped provider, which is why the adapter can
// promise a bound the provider could not.
func (b *boundedBlobs) BlobReaderCloseBound() time.Duration { return BoundedBlobReaderCloseBound }

// Get opens the provider reader eagerly and returns the pipe-backed reader.
func (b *boundedBlobs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	src, err := b.inner.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if src == nil {
		return nil, &NilBlobReaderError{Key: key}
	}
	pr, pw := io.Pipe()
	go pumpBlob(src, pw)
	return &boundedBlobReader{pipe: pr}, nil
}

// pumpBlob copies src into pw, releases src, then publishes the outcome.
//
// src is closed BEFORE the outcome is published, so a caller that sees io.EOF
// knows the provider reader was released cleanly, and a release failure is
// delivered instead of io.EOF. When the caller closed first, the copy fails
// with io.ErrClosedPipe as soon as the in-flight provider Read returns; until
// then this goroutine is the abandoned one the WithBoundedBlobReaders doc
// describes. PipeWriter.CloseWithError always returns nil.
func pumpBlob(src io.ReadCloser, pw *io.PipeWriter) {
	_, err := io.Copy(pw, src)
	if closeErr := src.Close(); err == nil {
		err = closeErr
	}
	_ = pw.CloseWithError(err) // always nil: the outcome goes to the reader, not back here
}

// boundedBlobReader is the caller-facing stream. io.PipeReader already makes
// Read and Close safe to call concurrently and releases a Read blocked at the
// moment of Close; this type adds the named terminal error and a latched Close.
type boundedBlobReader struct {
	pipe *io.PipeReader

	// closed is set before the pipe is closed, so any Read that observes the
	// closed pipe also observes closed and reports BlobReaderClosedError. A
	// provider error that merely happens to be io.ErrClosedPipe, arriving while
	// the reader is still open, passes through unrelabelled.
	closed atomic.Bool

	// once latches the first Close's outcome so every later Close reports the
	// same classification rather than re-running the teardown.
	once     sync.Once
	closeErr error
}

func (r *boundedBlobReader) Read(p []byte) (int, error) {
	n, err := r.pipe.Read(p)
	if errors.Is(err, io.ErrClosedPipe) && r.closed.Load() {
		return n, &BlobReaderClosedError{}
	}
	return n, err
}

// Close releases the caller without waiting for the pump; see
// WithBoundedBlobReaders for what is abandoned.
func (r *boundedBlobReader) Close() error {
	r.once.Do(func() {
		r.closed.Store(true)
		r.closeErr = r.pipe.CloseWithError(&BlobReaderClosedError{})
	})
	return r.closeErr
}
