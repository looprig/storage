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
	if err := (<-call.result).value; err != nil {
		t.Fatalf("Close = %v", err)
	}
	if !returned.Load() {
		t.Fatal("Close result was observable before closeReturned publication")
	}
}

func TestWaitTimedResultAcceptsFastCompletionObservedLate(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	result := make(chan timedResult[int], 1)
	result <- timedResult[int]{value: 42, finishedAt: deadline.Add(-time.Nanosecond)}
	got, outcome := waitTimedResult(context.Background(), result, deadline)
	if outcome != waitSucceeded || got.value != 42 {
		t.Fatalf("waitTimedResult = %#v, %v; want value 42, success", got, outcome)
	}
}

func TestWaitTimedResultReadyWinsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deadline := time.Now().Add(time.Second)
	result := make(chan timedResult[int], 1)
	result <- timedResult[int]{value: 7, finishedAt: deadline.Add(-time.Nanosecond)}
	got, outcome := waitTimedResult(ctx, result, deadline)
	if outcome != waitSucceeded || got.value != 7 {
		t.Fatalf("waitTimedResult = %#v, %v; want ready value 7 despite canceled observation context", got, outcome)
	}
}

func TestWaitTimedResultWithoutResultHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := make(chan timedResult[int])
	_, outcome := waitTimedResult(ctx, result, time.Now().Add(time.Second))
	if outcome != waitContextEnded {
		t.Fatalf("waitTimedResult outcome = %v, want context-ended", outcome)
	}
}

func TestWaitTimedResultRejectsActualLateCompletion(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	result := make(chan timedResult[int], 1)
	result <- timedResult[int]{value: 99, finishedAt: deadline.Add(time.Nanosecond)}
	got, outcome := waitTimedResult(context.Background(), result, deadline)
	if outcome != waitBoundExceeded || got.value != 99 {
		t.Fatalf("waitTimedResult = %#v, %v; want value 99 classified bound-exceeded", got, outcome)
	}
}

func TestWaitStartUsesActualStartAgainstContextDeadline(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	for _, tt := range []struct {
		name    string
		started time.Time
		wantOK  bool
	}{
		{name: "started before deadline", started: deadline.Add(-time.Nanosecond), wantOK: true},
		{name: "started after deadline", started: deadline.Add(time.Nanosecond), wantOK: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			started := make(chan time.Time, 1)
			started <- tt.started
			got, ok := waitStart(ctx, started)
			if !got.Equal(tt.started) || ok != tt.wantOK {
				t.Fatalf("waitStart = %v, %v; want %v, %v", got, ok, tt.started, tt.wantOK)
			}
		})
	}
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }
