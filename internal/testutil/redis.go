package testutil

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisImage = "redis:7-alpine"

var rd struct {
	once        sync.Once
	url         string
	containerID string
	err         error
}

// NewRedis returns a client for a shared throwaway Redis, closed when t ends.
// Tests isolate themselves by using keys of their own.
func NewRedis(t testing.TB) *redis.Client {
	t.Helper()
	rd.once.Do(startRedis)
	if rd.err != nil {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatalf("redis unavailable: %v", rd.err)
		}
		t.Skipf("redis unavailable (set TEST_REDIS_URL or start Docker): %v", rd.err)
	}
	opts, err := redis.ParseURL(rd.url)
	if err != nil {
		t.Fatalf("redis url: %v", err)
	}
	opts.ContextTimeoutEnabled = true
	client := redis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func startRedis() {
	if url := os.Getenv("TEST_REDIS_URL"); url != "" {
		rd.url = url
		return
	}
	if _, err := exec.LookPath("docker"); err != nil {
		rd.err = err
		return
	}
	out, err := exec.Command("docker", "run", "-d", "--rm",
		"--label", "llm-gateway-test=1",
		"-p", "127.0.0.1::6379",
		redisImage,
	).Output()
	if err != nil {
		rd.err = fmt.Errorf("docker run: %w", commandError(err))
		return
	}
	rd.containerID = strings.TrimSpace(string(out))

	out, err = exec.Command("docker", "port", rd.containerID, "6379/tcp").Output()
	if err != nil {
		rd.err = fmt.Errorf("docker port: %w", commandError(err))
		return
	}
	rd.url = "redis://" + strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])

	opts, err := redis.ParseURL(rd.url)
	if err != nil {
		rd.err = err
		return
	}
	client := redis.NewClient(opts)
	defer func() { _ = client.Close() }()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			rd.err = fmt.Errorf("redis did not become ready: %w", err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
