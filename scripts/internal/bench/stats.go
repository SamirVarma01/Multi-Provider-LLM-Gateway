package bench

import (
	"math"
	"slices"
	"time"
)

// Dist summarizes a set of durations, in milliseconds.
type Dist struct {
	N    int     `json:"n"`
	Mean float64 `json:"mean_ms"`
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
}

// Summarize computes exact percentiles (nearest rank) from every sample.
func Summarize(ds []time.Duration) Dist {
	if len(ds) == 0 {
		return Dist{}
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	return Dist{
		N:    len(s),
		Mean: ms(sum / time.Duration(len(s))),
		P50:  ms(rank(s, 0.50)),
		P95:  ms(rank(s, 0.95)),
		P99:  ms(rank(s, 0.99)),
		Max:  ms(s[len(s)-1]),
	}
}

// rank is the nearest-rank percentile of sorted s.
func rank(s []time.Duration, p float64) time.Duration {
	i := int(math.Ceil(p*float64(len(s)))) - 1
	return s[min(max(i, 0), len(s)-1)]
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Ms converts a duration to milliseconds.
func Ms(d time.Duration) float64 { return ms(d) }
