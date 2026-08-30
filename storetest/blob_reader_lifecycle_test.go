package storetest

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLifecycleLimitCapsHugeProviderBoundWithContextDiagnostic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	contextDeadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("test context has no deadline")
	}
	got := lifecycleLimit(ctx, time.Now(), 24*time.Hour)
	if !got.deadline.Equal(contextDeadline) || got.exceeded != waitContextEnded {
		t.Fatalf("lifecycleLimit = %#v, want context deadline %v and context-ended", got, contextDeadline)
	}
}

func TestLifecycleLimitUsesProviderBoundDiagnostic(t *testing.T) {
	started := time.Now()
	got := lifecycleLimit(context.Background(), started, time.Second)
	if want := started.Add(time.Second); !got.deadline.Equal(want) || got.exceeded != waitBoundExceeded {
		t.Fatalf("lifecycleLimit = %#v, want provider deadline %v and bound-exceeded", got, want)
	}
}

func TestLifecycleLimitEqualContextDeadlineUsesContextDiagnostic(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	got := lifecycleLimit(ctx, deadline.Add(-time.Second), time.Second)
	if !got.deadline.Equal(deadline) || got.exceeded != waitContextEnded {
		t.Fatalf("lifecycleLimit = %#v, want equal context deadline and context-ended", got)
	}
}

func TestLifecycleWatchdogIsDistinctFromOperationLimit(t *testing.T) {
	limit := timedLimit{deadline: time.Now().Add(time.Second), exceeded: waitBoundExceeded}
	watchdog, cancel := lifecycleWatchdog(context.Background(), limit)
	defer cancel()
	got, ok := watchdog.Deadline()
	if want := limit.deadline.Add(conformanceTimeout); !ok || !got.Equal(want) {
		t.Fatalf("watchdog deadline = %v, %v; want %v, true", got, ok, want)
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
	got, outcome := waitTimedResult(context.Background(), result, timedLimit{deadline: deadline, exceeded: waitBoundExceeded})
	if outcome != waitSucceeded || got.value != 42 {
		t.Fatalf("waitTimedResult = %#v, %v; want value 42, success", got, outcome)
	}
}

func TestWaitTimedResultAcceptsOnTimeCompletionPublishedAfterBound(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	result := make(chan timedResult[int])
	watchdog := newDoneSignalContext()
	type observation struct {
		got     timedResult[int]
		outcome waitOutcome
	}
	observed := make(chan observation, 1)
	go func() {
		got, outcome := waitTimedResult(watchdog, result, timedLimit{deadline: deadline, exceeded: waitBoundExceeded})
		observed <- observation{got: got, outcome: outcome}
	}()
	watchdog.requireEntered(t)
	select {
	case result <- timedResult[int]{value: 43, finishedAt: deadline.Add(-time.Nanosecond)}:
	case observedResult := <-observed:
		t.Fatalf("waitTimedResult returned before delayed publication: %#v", observedResult)
	}
	observedResult := <-observed
	got, outcome := observedResult.got, observedResult.outcome
	if outcome != waitSucceeded || got.value != 43 {
		t.Fatalf("waitTimedResult = %#v, %v; want delayed value 43, success", got, outcome)
	}
}

func TestWaitTimedResultPreservesLateCompletionPublishedAfterBound(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	result := make(chan timedResult[int])
	watchdog := newDoneSignalContext()
	type observation struct {
		got     timedResult[int]
		outcome waitOutcome
	}
	observed := make(chan observation, 1)
	go func() {
		got, outcome := waitTimedResult(watchdog, result, timedLimit{deadline: deadline, exceeded: waitBoundExceeded})
		observed <- observation{got: got, outcome: outcome}
	}()
	watchdog.requireEntered(t)
	select {
	case result <- timedResult[int]{value: 100, finishedAt: deadline.Add(time.Nanosecond)}:
	case observedResult := <-observed:
		t.Fatalf("waitTimedResult returned before delayed publication: %#v", observedResult)
	}
	observedResult := <-observed
	got, outcome := observedResult.got, observedResult.outcome
	if outcome != waitBoundExceeded || got.value != 100 {
		t.Fatalf("waitTimedResult = %#v, %v; want delayed value 100 classified bound-exceeded", got, outcome)
	}
}

func TestWaitTimedResultReadyWinsCanceledWatchdog(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deadline := time.Now().Add(time.Second)
	result := make(chan timedResult[int], 1)
	result <- timedResult[int]{value: 7, finishedAt: deadline.Add(-time.Nanosecond)}
	got, outcome := waitTimedResult(ctx, result, timedLimit{deadline: deadline, exceeded: waitBoundExceeded})
	if outcome != waitSucceeded || got.value != 7 {
		t.Fatalf("waitTimedResult = %#v, %v; want ready value 7 despite canceled watchdog", got, outcome)
	}
}

func TestWaitTimedResultWithoutResultUsesLimitWhenWatchdogEnds(t *testing.T) {
	for _, want := range []waitOutcome{waitBoundExceeded, waitContextEnded} {
		t.Run(map[waitOutcome]string{waitBoundExceeded: "provider bound", waitContextEnded: "context limit"}[want], func(t *testing.T) {
			watchdog, cancel := context.WithCancel(context.Background())
			cancel()
			result := make(chan timedResult[int])
			_, outcome := waitTimedResult(watchdog, result, timedLimit{deadline: time.Now().Add(-time.Second), exceeded: want})
			if outcome != want {
				t.Fatalf("waitTimedResult outcome = %v, want effective limit classification %v", outcome, want)
			}
		})
	}
}

func TestWaitTimedResultRejectsActualLateCompletion(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	result := make(chan timedResult[int], 1)
	result <- timedResult[int]{value: 99, finishedAt: deadline.Add(time.Nanosecond)}
	got, outcome := waitTimedResult(context.Background(), result, timedLimit{deadline: deadline, exceeded: waitBoundExceeded})
	if outcome != waitBoundExceeded || got.value != 99 {
		t.Fatalf("waitTimedResult = %#v, %v; want value 99 classified bound-exceeded", got, outcome)
	}
}

func TestWaitStartUsesActualStartAgainstContextDeadline(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
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
			got, outcome := waitStart(context.Background(), started, timedLimit{deadline: deadline, exceeded: waitContextEnded})
			wantOutcome := waitSucceeded
			if !tt.wantOK {
				wantOutcome = waitContextEnded
			}
			if !got.Equal(tt.started) || outcome != wantOutcome {
				t.Fatalf("waitStart = %v, %v; want %v, %v", got, outcome, tt.started, wantOutcome)
			}
		})
	}
}

func TestWaitStartAcceptsOnTimeStartPublishedAfterContextLimit(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	started := make(chan time.Time)
	watchdog := newDoneSignalContext()
	type observation struct {
		got     time.Time
		outcome waitOutcome
	}
	observed := make(chan observation, 1)
	go func() {
		got, outcome := waitStart(watchdog, started, timedLimit{deadline: deadline, exceeded: waitContextEnded})
		observed <- observation{got: got, outcome: outcome}
	}()
	watchdog.requireEntered(t)
	select {
	case started <- deadline.Add(-time.Nanosecond):
	case observedResult := <-observed:
		t.Fatalf("waitStart returned before delayed publication: %#v", observedResult)
	}
	observedResult := <-observed
	got, outcome := observedResult.got, observedResult.outcome
	if outcome != waitSucceeded || !got.Equal(deadline.Add(-time.Nanosecond)) {
		t.Fatalf("waitStart = %v, %v; want delayed on-time start, success", got, outcome)
	}
}

type doneSignalContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func newDoneSignalContext() *doneSignalContext {
	return &doneSignalContext{Context: context.Background(), entered: make(chan struct{})}
}

func (c *doneSignalContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func (c *doneSignalContext) requireEntered(t *testing.T) {
	t.Helper()
	select {
	case <-c.entered:
	case <-time.After(time.Second):
		t.Fatal("wait helper did not enter its blocking select")
	}
}

type nilReader struct{}

func (nilReader) Read([]byte) (int, error) { return 0, io.EOF }
