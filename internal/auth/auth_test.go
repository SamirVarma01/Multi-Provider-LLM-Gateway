package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/SamirVarma01/llm-gateway/internal/oai"
	"github.com/SamirVarma01/llm-gateway/internal/testutil"
)

func TestMain(m *testing.M) { testutil.Main(m) }

func TestGenerateKey(t *testing.T) {
	a, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := GenerateKey()
	if a == b {
		t.Fatal("two generated keys are equal")
	}
	if !strings.HasPrefix(a, KeyPrefix) || len(a) != len(KeyPrefix)+43 {
		t.Errorf("key %q: want %q + 43 base64url chars", a, KeyPrefix)
	}
	if got := DisplayPrefix(a); got != a[:9] {
		t.Errorf("DisplayPrefix = %q", got)
	}
	if !bytes.Equal(HashKey(a), HashKey(a)) || bytes.Equal(HashKey(a), HashKey(b)) || len(HashKey(a)) != 32 {
		t.Error("HashKey must be a deterministic 32-byte digest")
	}
}

// mapLookuper is an in-memory Lookuper keyed by plaintext.
type mapLookuper struct {
	keys  map[string]Key
	err   error
	calls atomic.Int32
}

func (m *mapLookuper) LookupByHash(_ context.Context, hash []byte) (Key, error) {
	m.calls.Add(1)
	if m.err != nil {
		return Key{}, m.err
	}
	for plain, k := range m.keys {
		if bytes.Equal(HashKey(plain), hash) {
			return k, nil
		}
	}
	return Key{}, ErrNotFound
}

func TestMiddleware(t *testing.T) {
	good, _ := GenerateKey()
	revoked, _ := GenerateKey()
	unknown, _ := GenerateKey()
	now := time.Now()
	goodKey := Key{ID: uuid.New(), Name: "good"}
	store := &mapLookuper{keys: map[string]Key{
		good:    goodKey,
		revoked: {ID: uuid.New(), RevokedAt: &now},
	}}

	var gotKey Key
	h := Middleware(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey, _ = FromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	tests := []struct {
		name   string
		header string
		status int
		code   string
	}{
		{"valid", "Bearer " + good, http.StatusNoContent, ""},
		{"scheme is case-insensitive", "bearer " + good, http.StatusNoContent, ""},
		{"missing header", "", http.StatusUnauthorized, "missing_api_key"},
		{"wrong scheme", "Basic " + good, http.StatusUnauthorized, "missing_api_key"},
		{"empty token", "Bearer ", http.StatusUnauthorized, "missing_api_key"},
		{"not a gateway key", "Bearer sk-abc", http.StatusUnauthorized, "invalid_api_key"},
		{"unknown key", "Bearer " + unknown, http.StatusUnauthorized, "invalid_api_key"},
		{"revoked key", "Bearer " + revoked, http.StatusUnauthorized, "invalid_api_key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey = Key{}
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != tt.status {
				t.Fatalf("status = %d, want %d (body %s)", rr.Code, tt.status, rr.Body)
			}
			if tt.code == "" {
				if gotKey.ID != goodKey.ID {
					t.Errorf("handler got key %v, want %v", gotKey.ID, goodKey.ID)
				}
				return
			}
			var body oai.ErrorBody
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatalf("error body is not JSON: %v", err)
			}
			if body.Error.Code != tt.code || body.Error.Type != oai.ErrTypeAuthentication {
				t.Errorf("error = %+v, want code %q", body.Error, tt.code)
			}
		})
	}
}

func TestMiddlewareStoreError(t *testing.T) {
	good, _ := GenerateKey()
	h := Middleware(&mapLookuper{err: errors.New("db down")})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("handler must not run when the store fails")
	}))
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Authorization", "Bearer "+good)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rr.Code)
	}
}

func TestCache(t *testing.T) {
	good, _ := GenerateKey()
	unknown, _ := GenerateKey()
	store := &mapLookuper{keys: map[string]Key{good: {ID: uuid.New()}}}
	c := NewCache(store, time.Minute)
	clock := time.Unix(1000, 0)
	c.now = func() time.Time { return clock }
	ctx := context.Background()

	for range 3 {
		if _, err := c.LookupByHash(ctx, HashKey(good)); err != nil {
			t.Fatal(err)
		}
	}
	if n := store.calls.Load(); n != 1 {
		t.Errorf("store calls within TTL = %d, want 1", n)
	}

	clock = clock.Add(time.Minute)
	if _, err := c.LookupByHash(ctx, HashKey(good)); err != nil {
		t.Fatal(err)
	}
	if n := store.calls.Load(); n != 2 {
		t.Errorf("store calls after TTL = %d, want 2", n)
	}

	for range 2 {
		if _, err := c.LookupByHash(ctx, HashKey(unknown)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	}
	if n := store.calls.Load(); n != 4 {
		t.Errorf("misses must not be cached: store calls = %d, want 4", n)
	}
}

func TestPGStore(t *testing.T) {
	pool := testutil.NewDatabase(t)
	s := NewPGStore(pool)
	ctx := context.Background()

	rpm, budget := 60, int64(5_000_000_000)
	k, plain, err := s.CreateKey(ctx, NewKey{Name: "alice", RPMLimit: &rpm, BudgetNUSD: &budget})
	if err != nil {
		t.Fatalf("CreateKey: %v", err)
	}
	if k.Name != "alice" || k.Prefix != DisplayPrefix(plain) || *k.RPMLimit != 60 ||
		k.TPMLimit != nil || *k.BudgetNUSD != budget || k.Revoked() {
		t.Errorf("created key = %+v", k)
	}

	var stored []byte
	if err := pool.QueryRow(ctx, `SELECT key_hash FROM api_keys WHERE id = $1`, k.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, HashKey(plain)) {
		t.Error("stored key_hash is not sha256(plaintext)")
	}

	got, err := s.LookupByHash(ctx, HashKey(plain))
	if err != nil || got.ID != k.ID {
		t.Fatalf("LookupByHash = %+v, %v", got, err)
	}
	if _, err := s.LookupByHash(ctx, HashKey("gw_nope")); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown key: err = %v, want ErrNotFound", err)
	}

	if _, _, err := s.CreateKey(ctx, NewKey{Name: "bob"}); err != nil {
		t.Fatal(err)
	}
	keys, err := s.ListKeys(ctx)
	if err != nil || len(keys) != 2 || keys[0].Name != "alice" || keys[1].Name != "bob" {
		t.Fatalf("ListKeys = %+v, %v", keys, err)
	}

	for range 2 { // revoking twice is a no-op
		rk, err := s.RevokeKey(ctx, k.ID)
		if err != nil || !rk.Revoked() {
			t.Fatalf("RevokeKey = %+v, %v", rk, err)
		}
	}
	got, _ = s.LookupByHash(ctx, HashKey(plain))
	if !got.Revoked() {
		t.Error("lookup after revoke does not show the key as revoked")
	}
	if _, err := s.RevokeKey(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("revoke unknown: err = %v, want ErrNotFound", err)
	}
}
