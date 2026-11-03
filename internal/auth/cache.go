package auth

import (
	"context"
	"sync"
	"time"
)

// Cache is a TTL cache in front of a Lookuper, so authenticated requests do
// not each cost a Postgres round trip. A revocation therefore takes effect
// within one TTL. Only successful lookups are cached; misses and errors always
// reach the backing store.
type Cache struct {
	next Lookuper
	ttl  time.Duration
	now  func() time.Time

	mu      sync.Mutex
	entries map[string]cacheEntry
	inserts int
}

type cacheEntry struct {
	key     Key
	expires time.Time
}

// sweepEvery bounds how many inserts happen between scans for expired entries.
const sweepEvery = 1024

// NewCache wraps next with a cache of the given TTL. A TTL <= 0 disables caching.
func NewCache(next Lookuper, ttl time.Duration) *Cache {
	return &Cache{next: next, ttl: ttl, now: time.Now, entries: make(map[string]cacheEntry)}
}

// LookupByHash implements Lookuper.
func (c *Cache) LookupByHash(ctx context.Context, hash []byte) (Key, error) {
	if c.ttl <= 0 {
		return c.next.LookupByHash(ctx, hash)
	}
	id := string(hash)
	now := c.now()

	c.mu.Lock()
	e, ok := c.entries[id]
	c.mu.Unlock()
	if ok && now.Before(e.expires) {
		return e.key, nil
	}

	k, err := c.next.LookupByHash(ctx, hash)
	if err != nil {
		return Key{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.inserts++
	if c.inserts%sweepEvery == 0 {
		for h, e := range c.entries {
			if !now.Before(e.expires) {
				delete(c.entries, h)
			}
		}
	}
	c.entries[id] = cacheEntry{key: k, expires: now.Add(c.ttl)}
	return k, nil
}
