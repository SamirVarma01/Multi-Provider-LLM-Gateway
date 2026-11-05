package router

import (
	"sync"
	"time"
)

// LatencyConfig configures latency tracking for the fastest policy
// (design §6.2).
type LatencyConfig struct {
	Alpha      float64       // weight of the newest sample, in (0, 1]
	StaleAfter time.Duration // an average whose newest sample is older than this is ignored
}

// DefaultLatencyConfig returns the defaults from design §6.2.
func DefaultLatencyConfig() LatencyConfig {
	return LatencyConfig{Alpha: 0.2, StaleAfter: 30 * time.Second}
}

// ewma is an exponentially weighted moving average.
type ewma struct {
	value float64
	at    time.Time // time of the newest sample; zero if none
}

// add folds in a sample. A sample arriving after the average went stale
// replaces it: an old average says little about the target now.
func (e *ewma) add(x float64, now time.Time, cfg LatencyConfig) {
	if e.at.IsZero() || now.Sub(e.at) > cfg.StaleAfter {
		e.value = x
	} else {
		e.value = cfg.Alpha*x + (1-cfg.Alpha)*e.value
	}
	e.at = now
}

// get returns the average if it has a sample newer than StaleAfter.
func (e *ewma) get(now time.Time, cfg LatencyConfig) (float64, bool) {
	if e.at.IsZero() || now.Sub(e.at) > cfg.StaleAfter {
		return 0, false
	}
	return e.value, true
}

// latency is what the gateway has measured of one target. Every request
// streams upstream (design D4), so every attempt that produces output gives
// a TTFT sample, and every completed one a decode-rate sample.
type latency struct {
	cfg LatencyConfig

	mu   sync.Mutex
	ttft ewma // seconds from the upstream call to its first output
	tps  ewma // output tokens per second after the first
}

func (l *latency) observeTTFT(d time.Duration, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ttft.add(d.Seconds(), now, l.cfg)
}

// observeDecode records a completed response of tokens output tokens whose
// first and last output were d apart. The first token's time is TTFT, so
// the rate is over the remaining tokens.
func (l *latency) observeDecode(tokens int, d time.Duration, now time.Time) {
	if tokens < 2 || d <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tps.add(float64(tokens-1)/d.Seconds(), now, l.cfg)
}

// estimates returns the fresh averages; ok is false for one with no
// recent sample.
func (l *latency) estimates(now time.Time) (ttft float64, ttftOK bool, tps float64, tpsOK bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ttft, ttftOK = l.ttft.get(now, l.cfg)
	tps, tpsOK = l.tps.get(now, l.cfg)
	return
}

// outputAverage tracks an alias's completion tokens per response, which
// predicts how long a non-streaming response will take to decode. It never
// goes stale: response lengths depend on the clients, not on target health.
type outputAverage struct {
	alpha float64

	mu    sync.Mutex
	value float64
	n     int
}

func (o *outputAverage) observe(tokens int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.n == 0 {
		o.value = float64(tokens)
	} else {
		o.value = o.alpha*float64(tokens) + (1-o.alpha)*o.value
	}
	o.n++
}

func (o *outputAverage) get() (float64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.value, o.n > 0
}
