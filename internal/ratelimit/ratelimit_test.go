package ratelimit

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func limit(n int) *int { return &n }

func newLimiter(t *testing.T) (*Limiter, *redis.Client) {
	t.Helper()
	rdb := testutil.NewRedis(t)
	return New(rdb, 2*time.Second), rdb
}

func admit(t *testing.T, l *Limiter, id uuid.UUID, lim Limits, tokens int) Decision {
	t.Helper()
	d, err := l.Admit(context.Background(), id, lim, tokens)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// level reads a bucket's stored tokens; a missing bucket is reported as -1.
func level(t *testing.T, rdb *redis.Client, key string) float64 {
	t.Helper()
	v, err := rdb.HGet(context.Background(), key, "tokens").Result()
	if err == redis.Nil {
		return -1
	}
	if err != nil {
		t.Fatal(err)
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestUnlimitedSkipsRedis(t *testing.T) {
	l := New(nil, time.Second) // a nil client would panic if touched
	if d, err := l.Admit(context.Background(), uuid.New(), Limits{}, 1<<30); err != nil || !d.Allowed {
		t.Errorf("Admit = %+v, %v", d, err)
	}
	if err := l.Adjust(context.Background(), uuid.New(), Limits{}, 5); err != nil {
		t.Error(err)
	}
}

func TestRPMBucket(t *testing.T) {
	l, _ := newLimiter(t)
	id := uuid.New()
	lim := Limits{RPM: limit(3)}
	for i := range 3 {
		if d := admit(t, l, id, lim, 0); !d.Allowed {
			t.Fatalf("request %d refused: %+v", i+1, d)
		}
	}
	d := admit(t, l, id, lim, 0)
	if d.Allowed || d.TooLarge {
		t.Fatalf("fourth request: %+v", d)
	}
	// 3 per minute is one every 20s.
	if d.RetryAfter <= 19*time.Second || d.RetryAfter > 20*time.Second {
		t.Errorf("RetryAfter = %v, want just under 20s", d.RetryAfter)
	}
}

func TestTPMBucket(t *testing.T) {
	l, _ := newLimiter(t)
	id := uuid.New()
	lim := Limits{TPM: limit(600)}
	if d := admit(t, l, id, lim, 500); !d.Allowed {
		t.Fatalf("first: %+v", d)
	}
	d := admit(t, l, id, lim, 400)
	// 100 left; 300 more at 10 tokens/s takes 30s.
	if d.Allowed || d.RetryAfter <= 29*time.Second || d.RetryAfter > 30*time.Second {
		t.Errorf("second: %+v, want a wait of just under 30s", d)
	}
	if d := admit(t, l, id, lim, 90); !d.Allowed {
		t.Errorf("a smaller request still fits: %+v", d)
	}
}

func TestRequestLargerThanTheLimitCanNeverPass(t *testing.T) {
	// No Redis needed: the answer cannot depend on the bucket.
	l := New(nil, time.Second)
	d, err := l.Admit(context.Background(), uuid.New(), Limits{TPM: limit(100)}, 101)
	if err != nil || d.Allowed || !d.TooLarge || d.RetryAfter != 0 {
		t.Errorf("Admit = %+v, %v", d, err)
	}
}

func TestRefusalTakesNothingFromEitherBucket(t *testing.T) {
	l, rdb := newLimiter(t)
	id := uuid.New()
	lim := Limits{RPM: limit(10), TPM: limit(100)}
	keys := bucketKeys(id)
	if d := admit(t, l, id, lim, 90); !d.Allowed {
		t.Fatal(d)
	}
	rpm, tpm := level(t, rdb, keys[0]), level(t, rdb, keys[1])
	if d := admit(t, l, id, lim, 50); d.Allowed {
		t.Fatal("the TPM bucket should refuse this")
	}
	if got := level(t, rdb, keys[0]); got != rpm {
		t.Errorf("rpm bucket went from %v to %v on a refusal", rpm, got)
	}
	if got := level(t, rdb, keys[1]); got != tpm {
		t.Errorf("tpm bucket went from %v to %v on a refusal", tpm, got)
	}
}

func TestBucketsRefill(t *testing.T) {
	l, rdb := newLimiter(t)
	id := uuid.New()
	lim := Limits{RPM: limit(6000)} // 100 per second
	if err := rdb.HSet(context.Background(), bucketKeys(id)[0], "tokens", 0, "ts_ms", time.Now().UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // about 5 tokens
	if d := admit(t, l, id, lim, 0); !d.Allowed {
		t.Errorf("bucket did not refill: %+v", d)
	}
}

func TestAdjustRefundsAndCharges(t *testing.T) {
	l, rdb := newLimiter(t)
	ctx := context.Background()
	id := uuid.New()
	lim := Limits{TPM: limit(6000)}
	key := bucketKeys(id)[1]

	if d := admit(t, l, id, lim, 5000); !d.Allowed {
		t.Fatal(d)
	}
	if err := l.Adjust(ctx, id, lim, 4000); err != nil { // used only 1000 of 5000
		t.Fatal(err)
	}
	if got := level(t, rdb, key); got < 5000 || got > 5100 {
		t.Errorf("after a refund: %v tokens, want about 5000", got)
	}

	if err := l.Adjust(ctx, id, lim, -9000); err != nil { // used far more than admitted
		t.Fatal(err)
	}
	if got := level(t, rdb, key); got > -3900 || got < -4100 {
		t.Errorf("after an overage: %v tokens, want a debt of about 4000", got)
	}
	d := admit(t, l, id, lim, 100)
	if d.Allowed || d.RetryAfter < 40*time.Second {
		t.Errorf("debt should delay the next request: %+v", d)
	}
	// The bucket lives until it would be full again, so debt outlasts a fixed TTL.
	ttl, err := rdb.PTTL(ctx, key).Result()
	if err != nil || ttl < 95*time.Second || ttl > 101*time.Second {
		t.Errorf("TTL = %v, %v; want the 100s it takes to refill", ttl, err)
	}

	// A refund past capacity leaves a full bucket, which is no bucket.
	if err := l.Adjust(ctx, id, lim, 1_000_000); err != nil {
		t.Fatal(err)
	}
	if got := level(t, rdb, key); got != -1 {
		t.Errorf("full bucket still stored: %v", got)
	}
}

// Atomic admission: however requests interleave, exactly the capacity passes.
func TestConcurrentAdmissionIsExact(t *testing.T) {
	l, _ := newLimiter(t)
	id := uuid.New()
	const capacity, tries = 20, 100
	lim := Limits{RPM: limit(capacity)}
	var ok atomic.Int64
	var wg sync.WaitGroup
	for range tries {
		wg.Go(func() {
			d, err := l.Admit(context.Background(), id, lim, 0)
			if err != nil {
				t.Error(err)
			}
			if d.Allowed {
				ok.Add(1)
			}
		})
	}
	wg.Wait()
	// 20 per minute refills one token every 3s, so nothing refills in this test.
	if ok.Load() != capacity {
		t.Errorf("admitted %d, want %d", ok.Load(), capacity)
	}
}

func TestRedisDownIsAnError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens here now
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	if _, err := New(rdb, time.Second).Admit(context.Background(), uuid.New(), Limits{RPM: limit(5)}, 1); err == nil {
		t.Error("Admit succeeded with Redis down")
	}
}

// A Redis that accepts connections and never answers must not hold the
// request for longer than the limiter's timeout.
func TestUnresponsiveRedisIsBounded(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conns sync.WaitGroup
	done := make(chan struct{})
	conns.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns.Go(func() {
				<-done
				c.Close()
			})
		}
	})
	rdb := redis.NewClient(&redis.Options{Addr: ln.Addr().String(), ContextTimeoutEnabled: true})
	t.Cleanup(func() {
		rdb.Close()
		close(done)
		ln.Close()
		conns.Wait()
	})

	start := time.Now()
	_, err = New(rdb, 100*time.Millisecond).Admit(context.Background(), uuid.New(), Limits{RPM: limit(5)}, 1)
	if err == nil {
		t.Error("Admit succeeded against a silent server")
	}
	if el := time.Since(start); el > time.Second {
		t.Errorf("Admit took %v, want about the 100ms timeout", el)
	}
}
