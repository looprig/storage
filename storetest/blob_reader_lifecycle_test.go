package storetest

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleDeadlineCapsHugeProviderBound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	contextDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("test context has no deadline")
	}
	got := lifecycleDeadline(ctx, time.Now(), 24*time.Hour)
	if !got.Equal(contextDeadline) {
		t.Fatalf("lifecycleDeadline = %v, want context deadline %v", got, contextDeadline)
	}
}

func TestBeginClosePublishesReturnBeforeResult(t *testing.T) {
	var returned atomic.Bool
	call := beginClose(io.NopCloser(nilReader{}), &returned)
	<-call.started
	if err := <-call.result; err != nil {
		t.Fatalf("Close = %v", err)
	}
	if !returned.Load() {
		t.Fatal("Close result was observable before closeReturned publication")
	}
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }
