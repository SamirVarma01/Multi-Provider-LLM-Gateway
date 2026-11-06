package ledger

import (
	"math"
	"math/bits"

	"github.com/SamirVarma01/llm-gateway/internal/provider"
)

// MaxCostNUSD is the most one request can cost: 1e9 USD, the largest budget
// a key can have. Cost saturates here, so a provider reporting an absurd
// token count cannot overflow an account's totals.
const MaxCostNUSD int64 = 1e18

// Cost returns what a request costs in nano-USD (design §8.1):
//
//	ceil((in·price.in + out·price.out) / 1000)
//
// Prices are micro-USD per 1M tokens, so dividing by 1000 gives nano-USD per
// token. The gateway, the reconciler, and reconcile_check all use this one
// function, so their results compare exactly. Negative counts are treated as
// zero. The arithmetic is 128-bit, so it cannot overflow before saturating.
func Cost(in, out int, p provider.Price) int64 {
	inHi, inLo := bits.Mul64(uint64(max(in, 0)), uint64(max(p.InputMicroPerM, 0)))
	outHi, outLo := bits.Mul64(uint64(max(out, 0)), uint64(max(p.OutputMicroPerM, 0)))
	lo, carry := bits.Add64(inLo, outLo, 0)
	hi, overflow := bits.Add64(inHi, outHi, carry)
	if overflow != 0 || hi >= 1000 { // bits.Div64 needs hi < divisor
		return MaxCostNUSD
	}
	q, rem := bits.Div64(hi, lo, 1000)
	if q >= uint64(math.MaxInt64) {
		return MaxCostNUSD
	}
	if rem > 0 {
		q++
	}
	return min(int64(q), MaxCostNUSD)
}
