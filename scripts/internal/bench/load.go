// Package bench holds what the benchmark scripts share: a load generator, a
// chat client that times each response, statistics, and small clients for
// the gateway's admin API, the mock provider's control API, and /metrics.
package bench

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Load is how requests are sent. With Rate set, a request starts every
// 1/Rate seconds whatever the earlier ones are doing (open loop), so a slow
// response cannot hold back the next request and hide its own slowness.
// Otherwise Workers goroutines each send one request after another (closed
// loop), which holds concurrency fixed and lets throughput vary.
type Load struct {
	Rate     float64
	Workers  int
	Warmup   time.Duration // sent but not measured
	Duration time.Duration // measured, after Warmup

	// OnMeasure, if set, is called in its own goroutine when the warmup ends.
	OnMeasure func()
	// Stop, if set, ends the load early when closed. Unlike cancelling the
	// context, it lets requests in flight finish.
	Stop <-chan struct{}
}

// Run is what Generate measured.
type Run struct {
	Results      []Result      // requests started after the warmup, in completion order
	Elapsed      time.Duration // measured time: from the end of the warmup until sending stopped
	InFlightMean float64       // requests in flight, sampled at each send
	InFlightMax  int64         // the most requests in flight at a send
	MaxLag       time.Duration // open loop: the most a send started behind schedule
}

// Send makes request number seq. Generate fills in Result.Intended.
type Send func(ctx context.Context, seq int) Result

// Generate applies l, then waits for the requests it started to finish. A
// request's latency is measured from Intended, when the schedule meant it to
// start, so a generator that falls behind adds to the latency it reports
// instead of hiding it.
func Generate(ctx context.Context, l Load, send Send) Run {
	g := &generator{send: send, results: make([]Result, 0, expected(l))}
	start := time.Now()
	g.measureFrom = start.Add(l.Warmup)
	end := g.measureFrom.Add(l.Duration)
	if l.OnMeasure != nil {
		t := time.AfterFunc(l.Warmup, l.OnMeasure)
		defer t.Stop()
	}
	g.stop = l.Stop
	// Sending ends at end, or earlier when Stop closes.
	stopped := end
	if l.Stop != nil {
		done := make(chan struct{})
		defer close(done)
		var at atomic.Int64
		go func() {
			select {
			case <-l.Stop:
				at.Store(time.Now().UnixNano())
			case <-done:
			}
		}()
		g.stoppedAt = &at
	}
	if l.Rate > 0 {
		g.openLoop(ctx, start, end, l.Rate)
	} else {
		g.closedLoop(ctx, end, max(l.Workers, 1))
	}
	g.wg.Wait()
	if g.stoppedAt != nil {
		if ns := g.stoppedAt.Load(); ns != 0 && time.Unix(0, ns).Before(end) {
			stopped = time.Unix(0, ns)
		}
	}
	run := Run{Results: g.results, Elapsed: max(stopped.Sub(g.measureFrom), 0), InFlightMax: g.inFlightMax, MaxLag: g.maxLag}
	if g.samples > 0 {
		run.InFlightMean = float64(g.inFlightSum) / float64(g.samples)
	}
	return run
}

// expected is how many results to make room for. Loads that are stopped
// early pass a long Duration, so it is capped.
func expected(l Load) int {
	if l.Rate > 0 {
		return min(int(l.Rate*l.Duration.Seconds())+1, 1<<16)
	}
	return 1024
}

type generator struct {
	send        Send
	stop        <-chan struct{} // nil never closes
	stoppedAt   *atomic.Int64   // when stop closed, in Unix ns; 0 before
	measureFrom time.Time
	wg          sync.WaitGroup

	inFlight    atomic.Int64
	inFlightMax int64 // written by the sending goroutine(s) under mu
	inFlightSum int64
	samples     int64
	maxLag      time.Duration

	mu      sync.Mutex
	results []Result
}

func (g *generator) openLoop(ctx context.Context, start, end time.Time, rate float64) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for seq := 0; ; seq++ {
		at := start.Add(time.Duration(float64(seq) / rate * float64(time.Second)))
		if !at.Before(end) {
			return
		}
		if d := time.Until(at); d > 0 {
			timer.Reset(d)
			select {
			case <-ctx.Done():
				return
			case <-g.stop:
				return
			case <-timer.C:
			}
		} else if g.stopped(ctx) {
			return
		}
		measured := !at.Before(g.measureFrom)
		if measured {
			g.maxLag = max(g.maxLag, time.Since(at))
		}
		g.wg.Add(1)
		go g.one(ctx, seq, at, measured)
	}
}

func (g *generator) closedLoop(ctx context.Context, end time.Time, workers int) {
	var seq atomic.Int64
	for range workers {
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			for !g.stopped(ctx) {
				at := time.Now()
				if !at.Before(end) {
					return
				}
				g.wg.Add(1)
				g.one(ctx, int(seq.Add(1)-1), at, !at.Before(g.measureFrom))
			}
		}()
	}
}

func (g *generator) stopped(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-g.stop:
		return true
	default:
		return false
	}
}

func (g *generator) one(ctx context.Context, seq int, at time.Time, measured bool) {
	defer g.wg.Done()
	n := g.inFlight.Add(1)
	defer g.inFlight.Add(-1)
	if measured {
		g.mu.Lock()
		g.inFlightSum += n
		g.samples++
		g.inFlightMax = max(g.inFlightMax, n)
		g.mu.Unlock()
	}
	r := g.send(ctx, seq)
	if !measured {
		return
	}
	r.Intended = at
	g.mu.Lock()
	g.results = append(g.results, r)
	g.mu.Unlock()
}
