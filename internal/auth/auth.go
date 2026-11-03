// Package auth issues and verifies gateway API keys.
//
// Keys are "gw_" followed by 32 random bytes in unpadded base64url. Only the
// SHA-256 of a key is stored: keys are high-entropy random strings, not
// passwords, so a slow KDF would add per-request cost for no security benefit.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"

	"github.com/google/uuid"
)

// KeyPrefix starts every gateway API key.
const KeyPrefix = "gw_"

// displayPrefixLen is how much of a key is stored in clear for display.
const displayPrefixLen = len(KeyPrefix) + 6

// ErrNotFound means no key matched.
var ErrNotFound = errors.New("auth: key not found")

// Key is an API key's stored metadata. It never contains the key itself.
type Key struct {
	ID         uuid.UUID
	Name       string
	Prefix     string
	RPMLimit   *int   // nil = unlimited
	TPMLimit   *int   // nil = unlimited
	BudgetNUSD *int64 // nil = unlimited; nano-USD
	CreatedAt  time.Time
	RevokedAt  *time.Time
}

// Revoked reports whether the key has been revoked.
func (k Key) Revoked() bool { return k.RevokedAt != nil }

// Lookuper finds a key by the SHA-256 of its plaintext.
type Lookuper interface {
	LookupByHash(ctx context.Context, hash []byte) (Key, error)
}

// GenerateKey returns a new random plaintext API key.
func GenerateKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return KeyPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// HashKey returns the stored form of a plaintext key.
func HashKey(plaintext string) []byte {
	h := sha256.Sum256([]byte(plaintext))
	return h[:]
}

// DisplayPrefix returns the part of a key that is safe to store and show.
func DisplayPrefix(plaintext string) string {
	if len(plaintext) <= displayPrefixLen {
		return plaintext
	}
	return plaintext[:displayPrefixLen]
}
