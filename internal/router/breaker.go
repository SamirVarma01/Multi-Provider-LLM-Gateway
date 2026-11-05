package router

import (
	"fmt"
	"sync"
	"time"
)

// BreakerConfig configures the circuit breaker kept for each target
// (design §6.3).
type BreakerConfig struct {
	WindowSize          int           // outcomes kept for the failure rate
	Window              time.Duration // outcomes older than this are ignored
	FailureRate         float64       // trip at or above this failure rate...
	MinRequests         int           // ...over at least this many outcomes in the window
	ConsecutiveFailures int           // or after this many failures in a row
	Cooldown            time.Duration // time open before the first probe
	MaxCooldown         time.Duration // cap for the cooldown, which doubles on each re-trip
	HalfOpenProbes      int           // attempts allowed at once while half-open
	HalfOpenSuccesses   int           // probe successes that close the breaker
}

// DefaultBreakerConfig returns the defaults from design §6.3.
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		WindowSize:          20,
		Window:              10 * time.Second,
		FailureRate:         0.5,
		MinRequests:         5,
		ConsecutiveFailures: 5,
		Cooldown:            5 * time.Second,
		MaxCooldown:         60 * time.Second,
		HalfOpenProbes:      1,
		HalfOpenSuccesses:   2,
	}
}

// BreakerState is a circuit breaker state.
type BreakerState int

// Breaker states.
const (
	StateClosed   BreakerState = iota // admitting every attempt
	StateOpen                         // admitting none until the cooldown ends
	StateHalfOpen                     // admitting a few probes to test recovery
)

func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	}
	return fmt.Sprintf("BreakerState(%d)", int(s))
}

// breaker is a circuit breaker for one target.
//
// Every admitted attempt reports exactly one result through its ticket:
// success, failure, or release (the attempt ended for a reason that says
// nothing about the target, such as the client leaving). A ticket admitted
// before a state change reports into a breaker that has moved on, so its
// result is ignored; the generation counter detects that.
type breaker struct {
	cfg      BreakerConfig
	now      func() time.Time
	onChange func(from, to BreakerState)

	mu          sync.Mutex
	state       BreakerState
	gen         uint64
	window      []sample // ring buffer of the last cfg.WindowSize outcomes
	next        int      // index the next outcome is written to
	consecutive int      // failures since the last success
	cooldown    time.Duration
	reopenAt    time.Time // when an open breaker admits its first probe
	probes      int       // probes in flight while half-open
	successes   int       // probe successes while half-open
	changes     []stateChange
}

type sample struct {
	at     time.Time
	failed bool
	valid  bool
}

type stateChange struct{ from, to BreakerState }

func newBreaker(cfg BreakerConfig, now func() time.Time, onChange func(from, to BreakerState)) *breaker {
	return &breaker{
		cfg:      cfg,
		now:      now,
		onChange: onChange,
		window:   make([]sample, cfg.WindowSize),
		cooldown: cfg.Cooldown,
	}
}

// ticket is permission for one attempt on a target.
type ticket struct {
	b     *breaker
	gen   uint64
	probe bool
	done  bool
}

// allow admits an attempt, or reports false if the breaker is open or every
// half-open probe slot is taken.
func (b *breaker) allow() (*ticket, bool) {
	b.mu.Lock()
	defer b.unlock()
	if b.state == StateOpen {
		if b.now().Before(b.reopenAt) {
			return nil, false
		}
		b.setState(StateHalfOpen)
	}
	if b.state == StateHalfOpen {
		if b.probes >= b.cfg.HalfOpenProbes {
			return nil, false
		}
		b.probes++
		return &ticket{b: b, gen: b.gen, probe: true}, true
	}
	return &ticket{b: b, gen: b.gen}, true
}

// success records that the attempt's target served it.
func (t *ticket) success() { t.report(false) }

// failure records that the target failed the attempt.
func (t *ticket) failure() { t.report(true) }

// release ends the attempt without judging the target.
func (t *ticket) release() {
	if t == nil || t.done {
		return
	}
	t.done = true
	b := t.b
	b.mu.Lock()
	defer b.unlock()
	if t.gen == b.gen && t.probe {
		b.probes--
	}
}

func (t *ticket) report(failed bool) {
	if t == nil || t.done {
		return
	}
	t.done = true
	b := t.b
	b.mu.Lock()
	defer b.unlock()
	if t.gen != b.gen {
		return
	}
	now := b.now()
	switch b.state {
	case StateClosed:
		b.window[b.next] = sample{at: now, failed: failed, valid: true}
		b.next = (b.next + 1) % len(b.window)
		if failed {
			b.consecutive++
		} else {
			b.consecutive = 0
		}
		if b.shouldTrip(now) {
			b.trip(now, b.cfg.Cooldown)
		}
	case StateHalfOpen:
		b.probes--
		if failed {
			b.trip(now, min(2*b.cooldown, b.cfg.MaxCooldown))
			return
		}
		b.successes++
		if b.successes >= b.cfg.HalfOpenSuccesses {
			b.cooldown = b.cfg.Cooldown
			b.setState(StateClosed)
		}
	}
}

func (b *breaker) shouldTrip(now time.Time) bool {
	if b.consecutive >= b.cfg.ConsecutiveFailures {
		return true
	}
	cutoff := now.Add(-b.cfg.Window)
	var total, failed int
	for _, s := range b.window {
		if s.valid && s.at.After(cutoff) {
			total++
			if s.failed {
				failed++
			}
		}
	}
	return total >= b.cfg.MinRequests && float64(failed) >= b.cfg.FailureRate*float64(total)
}

func (b *breaker) trip(now time.Time, cooldown time.Duration) {
	b.cooldown = cooldown
	b.reopenAt = now.Add(cooldown)
	b.setState(StateOpen)
}

// setState moves to a new state with fresh counters. Callers hold b.mu.
func (b *breaker) setState(s BreakerState) {
	b.changes = append(b.changes, stateChange{b.state, s})
	b.state = s
	b.gen++
	clear(b.window)
	b.next, b.consecutive, b.probes, b.successes = 0, 0, 0, 0
}

// unlock releases b.mu, then reports state changes, so the callback never
// runs under the lock.
func (b *breaker) unlock() {
	changes := b.changes
	b.changes = nil
	b.mu.Unlock()
	if b.onChange != nil {
		for _, c := range changes {
			b.onChange(c.from, c.to)
		}
	}
}

// snapshot returns the state and, for an open breaker, how long until it
// admits a probe.
func (b *breaker) snapshot() (BreakerState, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateOpen {
		return b.state, 0
	}
	return b.state, max(b.reopenAt.Sub(b.now()), 0)
}
