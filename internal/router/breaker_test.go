package router

import (
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct{ t time.Time }

func newFakeClock() *fakeClock                { return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} }
func (c *fakeClock) now() time.Time           { return c.t }
func (c *fakeClock) advance(d time.Duration)  { c.t = c.t.Add(d) }
func (b *breaker) currentState() BreakerState { s, _ := b.snapshot(); return s }

func testBreaker(clk *fakeClock, changes *[]stateChange) *breaker {
	return newBreaker(DefaultBreakerConfig(), clk.now, func(from, to BreakerState) {
		if changes != nil {
			*changes = append(*changes, stateChange{from, to})
		}
	})
}

// run admits one attempt and reports its result.
func run(t *testing.T, b *breaker, failed bool) {
	t.Helper()
	tk, ok := b.allow()
	if !ok {
		t.Fatal("attempt not admitted")
	}
	if failed {
		tk.failure()
	} else {
		tk.success()
	}
}

func TestBreakerTripsOnConsecutiveFailures(t *testing.T) {
	clk := newFakeClock()
	var changes []stateChange
	b := testBreaker(clk, &changes)
	for range 4 {
		run(t, b, true)
		clk.advance(time.Minute) // outside the window: only the streak counts
	}
	if s := b.currentState(); s != StateClosed {
		t.Fatalf("after 4 failures: %s", s)
	}
	run(t, b, true)
	if s := b.currentState(); s != StateOpen {
		t.Fatalf("after 5 failures in a row: %s", s)
	}
	if _, ok := b.allow(); ok {
		t.Error("open breaker admitted an attempt")
	}
	if len(changes) != 1 || changes[0] != (stateChange{StateClosed, StateOpen}) {
		t.Errorf("changes = %v", changes)
	}
}

func TestBreakerSuccessResetsStreak(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	for range 4 {
		run(t, b, true)
		clk.advance(time.Minute)
	}
	run(t, b, false)
	for range 4 {
		run(t, b, true)
		clk.advance(time.Minute)
	}
	if s := b.currentState(); s != StateClosed {
		t.Errorf("state = %s, want closed: the streak was broken", s)
	}
}

func TestBreakerTripsOnFailureRate(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	// 2 failures out of 4 is 50%, but below the minimum volume of 5.
	for _, failed := range []bool{true, false, true, false} {
		run(t, b, failed)
	}
	if s := b.currentState(); s != StateClosed {
		t.Fatalf("below min volume: %s", s)
	}
	run(t, b, true) // 3 of 5
	if s := b.currentState(); s != StateOpen {
		t.Errorf("3 of 5 failed: %s, want open", s)
	}
}

func TestBreakerFailureRateBelowThreshold(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	for i := range 20 {
		run(t, b, i%3 == 0) // 7 of 20 failed: 35%
	}
	if s := b.currentState(); s != StateClosed {
		t.Errorf("35%% failures: %s, want closed", s)
	}
}

func TestBreakerIgnoresOldOutcomes(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	for _, failed := range []bool{true, true, false, false} {
		run(t, b, failed)
	}
	clk.advance(11 * time.Second) // the window is 10s
	for _, failed := range []bool{true, false, false, false} {
		run(t, b, failed)
	}
	// In the window: 1 of 4 failed (below min volume, and 25%).
	if s := b.currentState(); s != StateClosed {
		t.Errorf("state = %s, want closed: old failures should have expired", s)
	}
}

func TestBreakerWindowKeepsLastN(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	for range 30 {
		run(t, b, false)
	}
	for range 2 {
		for _, failed := range []bool{true, true, true, true, false} {
			run(t, b, failed)
		}
	}
	if s := b.currentState(); s != StateClosed {
		t.Fatalf("8 of the last 20 failed: %s, want closed", s)
	}
	run(t, b, true)
	run(t, b, true)
	// 10 of the last 20 failed. Over all 42 outcomes it would be 24%.
	if s := b.currentState(); s != StateOpen {
		t.Errorf("state = %s, want open", s)
	}
}

func TestBreakerHalfOpenRecovery(t *testing.T) {
	clk := newFakeClock()
	var changes []stateChange
	b := testBreaker(clk, &changes)
	for range 5 {
		run(t, b, true)
	}
	if _, retry := b.snapshot(); retry != 5*time.Second {
		t.Errorf("retry in %v, want 5s", retry)
	}
	clk.advance(5 * time.Second)

	probe, ok := b.allow()
	if !ok || b.currentState() != StateHalfOpen {
		t.Fatalf("after cooldown: admitted=%v state=%s", ok, b.currentState())
	}
	if _, ok := b.allow(); ok {
		t.Fatal("half-open admitted a second concurrent probe")
	}
	probe.success()
	if s := b.currentState(); s != StateHalfOpen {
		t.Fatalf("after 1 probe success: %s, want half-open (needs 2)", s)
	}
	run(t, b, false)
	if s := b.currentState(); s != StateClosed {
		t.Fatalf("after 2 probe successes: %s", s)
	}
	want := []stateChange{{StateClosed, StateOpen}, {StateOpen, StateHalfOpen}, {StateHalfOpen, StateClosed}}
	if len(changes) != len(want) {
		t.Fatalf("changes = %v, want %v", changes, want)
	}
	for i := range want {
		if changes[i] != want[i] {
			t.Errorf("changes = %v, want %v", changes, want)
		}
	}
}

func TestBreakerProbeFailureDoublesCooldown(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	for range 5 {
		run(t, b, true)
	}
	for _, want := range []time.Duration{10, 20, 40, 60, 60} {
		_, retry := b.snapshot()
		clk.advance(retry)
		run(t, b, true) // the probe fails
		if _, retry := b.snapshot(); retry != want*time.Second {
			t.Fatalf("cooldown = %v, want %v", retry, want*time.Second)
		}
	}

	// A full close resets the cooldown.
	clk.advance(time.Minute)
	run(t, b, false)
	run(t, b, false)
	for range 5 {
		run(t, b, true)
	}
	if _, retry := b.snapshot(); retry != 5*time.Second {
		t.Errorf("cooldown after close and re-trip = %v, want 5s", retry)
	}
}

func TestBreakerReleaseFreesProbe(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	for range 5 {
		run(t, b, true)
	}
	clk.advance(5 * time.Second)
	probe, _ := b.allow()
	probe.release()
	probe.failure() // a ticket reports once; this is ignored
	if s := b.currentState(); s != StateHalfOpen {
		t.Fatalf("state = %s, want half-open", s)
	}
	if _, ok := b.allow(); !ok {
		t.Error("released probe slot was not freed")
	}
}

func TestBreakerIgnoresStaleTickets(t *testing.T) {
	clk := newFakeClock()
	b := testBreaker(clk, nil)
	slow, _ := b.allow() // admitted while closed, finishes later
	for range 5 {
		run(t, b, true)
	}
	clk.advance(5 * time.Second)
	probe, _ := b.allow()

	slow.success() // from before the trip: must not count as a probe success
	probe.success()
	if s := b.currentState(); s != StateHalfOpen {
		t.Errorf("state = %s, want half-open: only one probe has succeeded", s)
	}
}
