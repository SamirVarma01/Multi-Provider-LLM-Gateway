// Package testutil provides throwaway infrastructure for integration tests.
//
// Postgres comes from TEST_DATABASE_URL if set: a server URL with no database
// name or query, e.g. postgres://postgres:test@localhost:5432, for a user that
// may CREATE DATABASE. Otherwise a container is started with the
// docker CLI on first use and removed by Main. Running containers through the
// CLI instead of a Docker SDK keeps the test process free of background
// goroutines, so goleak checks stay meaningful.
//
// When neither is available, tests that need Postgres are skipped, unless
// REQUIRE_INTEGRATION=1, in which case they fail.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/goleak"

	"github.com/SamirVarma01/llm-gateway/internal/db"
	"github.com/SamirVarma01/llm-gateway/migrations"
)

const postgresImage = "postgres:17-alpine"

var pg struct {
	once        sync.Once
	baseURL     string // connection URL without a database name
	containerID string
	err         error
}

// Main runs the package's tests, removes any container started for them, and
// then fails the run if goroutines leaked. Use it as the body of TestMain.
func Main(m *testing.M) {
	code := m.Run()
	if pg.containerID != "" {
		_ = exec.Command("docker", "rm", "-f", pg.containerID).Run()
	}
	if code == 0 {
		if err := goleak.Find(); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: %v\n", err)
			code = 1
		}
	}
	os.Exit(code)
}

// NewDatabase creates a fresh, fully migrated database for t and returns a pool
// for it. The pool is closed when t ends.
func NewDatabase(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pg.once.Do(startPostgres)
	if pg.err != nil {
		if os.Getenv("REQUIRE_INTEGRATION") == "1" {
			t.Fatalf("postgres unavailable: %v", pg.err)
		}
		t.Skipf("postgres unavailable (set TEST_DATABASE_URL or start Docker): %v", pg.err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	name := "t_" + randomHex(8)
	admin, err := pgx.Connect(ctx, pg.baseURL+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("connect admin db: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}

	pool, err := db.Open(ctx, pg.baseURL+"/"+name+"?sslmode=disable")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := db.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func startPostgres() {
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		pg.baseURL = strings.TrimSuffix(url, "/")
		return
	}
	if _, err := exec.LookPath("docker"); err != nil {
		pg.err = err
		return
	}
	out, err := exec.Command("docker", "run", "-d", "--rm",
		"--label", "llm-gateway-test=1",
		"-e", "POSTGRES_PASSWORD=test",
		"-p", "127.0.0.1::5432",
		postgresImage,
		// Durability is irrelevant for throwaway test data.
		"-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off",
	).Output()
	if err != nil {
		pg.err = fmt.Errorf("docker run: %w", commandError(err))
		return
	}
	pg.containerID = strings.TrimSpace(string(out))

	out, err = exec.Command("docker", "port", pg.containerID, "5432/tcp").Output()
	if err != nil {
		pg.err = fmt.Errorf("docker port: %w", commandError(err))
		return
	}
	// "127.0.0.1:49153" (possibly several lines, one per address family).
	hostPort := strings.TrimSpace(strings.SplitN(string(out), "\n", 2)[0])
	pg.baseURL = "postgres://postgres:test@" + hostPort

	// The image's init phase listens only on a unix socket, so the first
	// successful TCP connection means the real server is up.
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, pg.baseURL+"/postgres?sslmode=disable")
		if err == nil {
			_ = conn.Close(ctx)
			cancel()
			return
		}
		cancel()
		if time.Now().After(deadline) {
			pg.err = fmt.Errorf("postgres did not become ready: %w", err)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func commandError(err error) error {
	var ee *exec.ExitError
	if errors.As(err, &ee) && len(ee.Stderr) > 0 {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(ee.Stderr)))
	}
	return err
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
