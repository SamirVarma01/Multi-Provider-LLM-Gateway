package ledger

import (
	"math"
	"testing"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

func TestCost(t *testing.T) {
	price := func(in, out int64) provider.Price {
		return provider.Price{InputMicroPerM: in, OutputMicroPerM: out}
	}
	tests := []struct {
		name    string
		in, out int
		p       provider.Price
		want    int64
	}{
		{"nothing", 0, 0, price(150_000, 600_000), 0},
		{"free model", 1000, 1000, price(0, 0), 0},
		// $0.15/MTok is 150 nano-USD per token, $0.60/MTok is 600.
		{"exact", 10, 2, price(150_000, 600_000), 10*150 + 2*600},
		{"rounds up", 1, 0, price(1, 0), 1},
		{"rounds up once over both sides", 1, 1, price(1, 1), 1},
		{"exact thousand does not round", 1000, 0, price(1, 0), 1},
		{"just over a thousand", 1001, 0, price(1, 0), 2},
		{"negative counts are zero", -5, 2, price(150_000, 600_000), 1200},
		{"beyond int64 before dividing", math.MaxInt32, math.MaxInt32, price(1e12, 1e12), MaxCostNUSD},
		{"saturates", math.MaxInt, math.MaxInt, price(math.MaxInt64, math.MaxInt64), MaxCostNUSD},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Cost(tt.in, tt.out, tt.p); got != tt.want {
				t.Errorf("Cost(%d, %d, %+v) = %d, want %d", tt.in, tt.out, tt.p, got, tt.want)
			}
		})
	}
}
