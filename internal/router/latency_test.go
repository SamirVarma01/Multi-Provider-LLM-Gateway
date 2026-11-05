package router

import (
	"math"
	"testing"
	"time"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestLatencyEWMA(t *testing.T) {
	clk := newFakeClock()
	l := &latency{cfg: DefaultLatencyConfig()}
	if _, ok, _, _ := l.estimates(clk.now()); ok {
		t.Fatal("no samples, but TTFT reported")
	}
	l.observeTTFT(100*time.Millisecond, clk.now())
	l.observeTTFT(200*time.Millisecond, clk.now())
	ttft, ok, _, tpsOK := l.estimates(clk.now())
	// The first sample seeds the average; then 0.2·0.2 + 0.8·0.1.
	if !ok || !near(ttft, 0.12) {
		t.Errorf("ttft = %v, %v; want 0.12", ttft, ok)
	}
	if tpsOK {
		t.Error("decode rate reported without a sample")
	}

	l.observeDecode(11, 500*time.Millisecond, clk.now()) // 10 tokens after the first in 0.5s
	if _, _, tps, ok := l.estimates(clk.now()); !ok || !near(tps, 20) {
		t.Errorf("tps = %v, %v; want 20", tps, ok)
	}
	l.observeDecode(1, time.Second, clk.now()) // one token: no rate
	l.observeDecode(5, 0, clk.now())           // no duration: no rate
	if _, _, tps, _ := l.estimates(clk.now()); !near(tps, 20) {
		t.Errorf("tps = %v after unusable samples, want 20", tps)
	}
}

func TestLatencyStale(t *testing.T) {
	clk := newFakeClock()
	l := &latency{cfg: DefaultLatencyConfig()}
	l.observeTTFT(time.Second, clk.now())
	clk.advance(30 * time.Second)
	if _, ok, _, _ := l.estimates(clk.now()); !ok {
		t.Fatal("a sample exactly 30s old should still count")
	}
	clk.advance(time.Millisecond)
	if _, ok, _, _ := l.estimates(clk.now()); ok {
		t.Fatal("stale average reported")
	}
	// A new sample replaces the stale average instead of blending with it.
	l.observeTTFT(100*time.Millisecond, clk.now())
	if ttft, ok, _, _ := l.estimates(clk.now()); !ok || !near(ttft, 0.1) {
		t.Errorf("ttft = %v, %v; want 0.1", ttft, ok)
	}
}

func TestOutputAverage(t *testing.T) {
	o := &outputAverage{alpha: 0.5}
	if _, ok := o.get(); ok {
		t.Fatal("no samples, but an average reported")
	}
	o.observe(100)
	o.observe(200)
	if v, ok := o.get(); !ok || !near(v, 150) {
		t.Errorf("average = %v, %v; want 150", v, ok)
	}
}
