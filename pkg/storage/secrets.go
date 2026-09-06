package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Cipher provides at-rest encryption for secret setting values (AES-256-GCM).
// Encrypted values are stored as "enc:v1:<base64(nonce|ciphertext)>" and are
// transparently decrypted on read. Production deployments should inject an
// external key (for cmd/server, HARNESS_MASTER_KEY) from a KMS, vault or OS
// key store. EnsureMasterKey is an embedded/demo fallback whose key lives in
// the same database as the ciphertext and therefore does not protect a stolen
// database file.
//
// Note on scope: this is not a full KMS. It removes the "plaintext secret in
// the settings table" finding; key rotation and HSM integration are the
// documented next step.
type Cipher struct {
	aead cipher.AEAD
}

// SecretPrefix marks an encrypted setting value.
const SecretPrefix = "enc:v1:"

// NewCipher builds a Cipher from a 32-byte hex-encoded master key.
func NewCipher(keyHex string) (*Cipher, error) {
	key, err := hexDecodeKey(keyHex)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead}, nil
}

func hexDecodeKey(key string) ([]byte, error) {
	if decoded, err := base64.StdEncoding.DecodeString(key); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if decoded, err := hex.DecodeString(key); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	return nil, fmt.Errorf("invalid master key: expected a 32-byte base64 or hex key")
}

// Encrypt seals a plaintext value. Empty values pass through unchanged.
func (c *Cipher) Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if strings.HasPrefix(plaintext, SecretPrefix) {
		return plaintext, nil // already sealed
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := c.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return SecretPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt opens a sealed value; unsealed values pass through so settings
// written before encryption was enabled remain readable.
func (c *Cipher) Decrypt(value string) (string, error) {
	if !strings.HasPrefix(value, SecretPrefix) {
		return value, nil
	}
	sealed, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, SecretPrefix))
	if err != nil {
		return "", fmt.Errorf("decode sealed setting: %w", err)
	}
	if len(sealed) < c.aead.NonceSize() {
		return "", errors.New("sealed setting too short")
	}
	nonce, ciphertext := sealed[:c.aead.NonceSize()], sealed[c.aead.NonceSize():]
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("open sealed setting: %w", err)
	}
	return string(plaintext), nil
}

// EnsureMasterKey returns the embedded/demo master key, generating and
// persisting one on first use. Do not use this fallback when database-file
// compromise is in scope; inject a key from an external secret system instead.
func EnsureMasterKey(ctx context.Context, db *sql.DB, dialect SQLDialect) (string, error) {
	if db == nil {
		return "", fmt.Errorf("master key requires a database handle")
	}
	if err := validateSQLDialect(dialect); err != nil {
		return "", err
	}
	const key = "secret_key"
	selectQuery := sqlQuery{"SELECT value FROM store_meta WHERE key = ?"}
	insertQuery := sqlQuery{"INSERT INTO store_meta (key, value) VALUES ('secret_key', ?)"}
	var value string
	err := db.QueryRowContext(ctx, selectQuery.bind(dialect), key).Scan(&value)
	if err == nil && strings.TrimSpace(value) != "" {
		return value, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	generated := base64.StdEncoding.EncodeToString(raw)
	if _, err := db.ExecContext(ctx, insertQuery.bind(dialect), generated); err != nil {
		// A concurrent first-boot may have inserted the key first: re-read
		// instead of failing, so both instances converge on one key.
		var existing string
		if retryErr := db.QueryRowContext(ctx, selectQuery.bind(dialect), key).Scan(&existing); retryErr != nil {
			return "", fmt.Errorf("persist master key: %w", err)
		}
		return existing, nil
	}
	return generated, nil
}
