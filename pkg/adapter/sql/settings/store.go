// Package settings provides the SQL adapter for application deployment
// settings. It does not run schema migrations; callers must provision the
// shared settings table before constructing a Store.
package settings

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/whhhh1500/auto-agent/pkg/adapter/sql/sqlkit"
	appsettings "github.com/whhhh1500/auto-agent/pkg/app/settings"
)

const (
	defaultMaxEntries    = 256
	defaultMaxValueBytes = 256 << 10
	maxConfigEntries     = 1 << 16
	maxConfigValueBytes  = 64 << 20
	maxSettingKeyRunes   = 128
)

// Cipher seals persisted settings and opens them on read. The application
// adapter deliberately knows no key-management or encryption implementation.
type Cipher interface {
	Encrypt(string) (string, error)
	Decrypt(string) (string, error)
}

// Options configures a settings Store. Zero MaxEntries and MaxValueBytes use
// the legacy-compatible limits of 256 entries and 256 KiB respectively.
type Options struct {
	DB            *sql.DB
	Dialect       sqlkit.Dialect
	Cipher        Cipher
	MaxEntries    int
	MaxValueBytes int
}

// Store persists application settings in the shared settings table.
type Store struct {
	db            *sql.DB
	dialect       sqlkit.Dialect
	cipher        Cipher
	maxEntries    int
	maxValueBytes int
}

// Cipher exposes the already-composed encryption seam to sibling adapters in
// the composition root. It never exposes key material.
func (store *Store) Cipher() Cipher {
	if store == nil {
		return nil
	}
	return store.cipher
}

var _ appsettings.Repository = (*Store)(nil)
var _ appsettings.AbsentSettingCreator = (*Store)(nil)
var _ appsettings.SettingCompareAndSwapper = (*Store)(nil)

// New validates SQL and bound configuration without inspecting or migrating
// the database schema.
func New(options Options) (*Store, error) {
	if options.DB == nil {
		return nil, fmt.Errorf("sql settings store requires a database handle")
	}
	if !options.Dialect.Valid() {
		return nil, fmt.Errorf("unsupported SQL dialect %q", options.Dialect.String())
	}
	maxEntries, err := configuredLimit("max settings entries", options.MaxEntries, defaultMaxEntries, maxConfigEntries)
	if err != nil {
		return nil, err
	}
	maxValueBytes, err := configuredLimit("max setting value bytes", options.MaxValueBytes, defaultMaxValueBytes, maxConfigValueBytes)
	if err != nil {
		return nil, err
	}
	return &Store{
		db: options.DB, dialect: options.Dialect, cipher: options.Cipher,
		maxEntries: maxEntries, maxValueBytes: maxValueBytes,
	}, nil
}

// SetSetting validates, optionally seals, and upserts one raw setting value.
// At capacity, replacing an existing key remains permitted.
func (store *Store) SetSetting(ctx context.Context, key, value string) error {
	if err := validateKey(key); err != nil {
		return err
	}
	value, err := store.persistedValue(value)
	if err != nil {
		return err
	}

	var stored int
	if err := store.db.QueryRowContext(ctx, store.bind(sqlCountSettings)).Scan(&stored); err != nil {
		return err
	}
	if stored >= store.maxEntries {
		var existing string
		getErr := store.db.QueryRowContext(ctx, store.bind(sqlGetSetting), key).Scan(&existing)
		if getErr != nil {
			if !errors.Is(getErr, sql.ErrNoRows) {
				return getErr
			}
			return fmt.Errorf("settings exceed maximum of %d", store.maxEntries)
		}
	}
	_, err = store.db.ExecContext(ctx, store.bind(sqlSetSetting), key, value)
	return err
}

// SetSettingIfAbsent validates and optionally seals value, then atomically
// creates key without replacing an existing value. It is the first-writer-wins
// primitive for one-time bootstrap imports.
func (store *Store) SetSettingIfAbsent(ctx context.Context, key, value string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	value, err := store.persistedValue(value)
	if err != nil {
		return false, err
	}
	result, err := store.db.ExecContext(ctx, store.bind(sqlSetSettingIfAbsent), key, value, store.maxEntries)
	if err != nil {
		return false, err
	}
	created, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if created > 0 {
		return true, nil
	}
	var existing string
	err = store.db.QueryRowContext(ctx, store.bind(sqlGetSetting), key).Scan(&existing)
	if err == nil {
		return false, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("settings exceed maximum of %d", store.maxEntries)
	}
	return false, err
}

// CompareAndSwapSetting replaces a setting only when its current plaintext
// equals expectedPlaintext. The final UPDATE predicates on the raw stored
// ciphertext, so concurrent writers cannot bypass the comparison even when a
// Cipher uses randomized encryption.
func (store *Store) CompareAndSwapSetting(ctx context.Context, key, expectedPlaintext, nextPlaintext string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	if err := store.validateValue(expectedPlaintext); err != nil {
		return false, err
	}
	nextStored, err := store.persistedValue(nextPlaintext)
	if err != nil {
		return false, err
	}
	var currentStored string
	err = store.db.QueryRowContext(ctx, store.bind(sqlGetSetting), key).Scan(&currentStored)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	currentPlaintext := currentStored
	if store.cipher != nil {
		currentPlaintext, err = store.cipher.Decrypt(currentStored)
		if err != nil {
			return false, err
		}
	}
	if currentPlaintext != expectedPlaintext {
		return false, nil
	}
	result, err := store.db.ExecContext(ctx, store.bind(sqlCompareAndSwapSetting), nextStored, key, currentStored)
	if err != nil {
		return false, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return updated > 0, nil
}

// GetSetting returns a raw persisted setting value after optional decryption.
func (store *Store) GetSetting(ctx context.Context, key string) (string, bool, error) {
	if err := validateKey(key); err != nil {
		return "", false, err
	}
	var value string
	err := store.db.QueryRowContext(ctx, store.bind(sqlGetSetting), key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if store.cipher != nil {
		plain, err := store.cipher.Decrypt(value)
		if err != nil {
			return "", false, err
		}
		value = plain
	}
	return value, true, nil
}

func (store *Store) validateValue(value string) error {
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("setting value contains NUL")
	}
	if len(value) > store.maxValueBytes {
		return fmt.Errorf("setting value exceeds %d bytes", store.maxValueBytes)
	}
	return nil
}

func (store *Store) persistedValue(value string) (string, error) {
	if err := store.validateValue(value); err != nil {
		return "", err
	}
	if store.cipher != nil {
		sealed, err := store.cipher.Encrypt(value)
		if err != nil {
			return "", err
		}
		value = sealed
	}
	if len(value) > store.maxValueBytes {
		return "", fmt.Errorf("setting value exceeds %d bytes", store.maxValueBytes)
	}
	return value, nil
}

func (store *Store) bind(query string) string {
	bound, err := sqlkit.Bind(query, store.dialect)
	if err != nil {
		panic("settings SQL binding invariant violated: " + err.Error())
	}
	return bound
}

func configuredLimit(name string, value, fallback, maximum int) (int, error) {
	if value == 0 {
		return fallback, nil
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must not be negative", name)
	}
	if value > maximum {
		return 0, fmt.Errorf("%s must not exceed %d", name, maximum)
	}
	return value, nil
}

func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("setting key is empty")
	}
	if strings.ContainsRune(key, '\x00') {
		return fmt.Errorf("setting key contains NUL")
	}
	for _, character := range key {
		if unicode.IsControl(character) {
			return fmt.Errorf("setting key contains a control character")
		}
	}
	if utf8.RuneCountInString(key) > maxSettingKeyRunes {
		return fmt.Errorf("setting key exceeds %d runes", maxSettingKeyRunes)
	}
	return nil
}

const (
	sqlSetSetting = "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value"
	// The conditional SELECT preserves the existing entry-cap while the
	// conflict clause makes one-time imports first-writer-wins across processes.
	sqlSetSettingIfAbsent    = "INSERT INTO settings (key, value) SELECT ?, ? WHERE (SELECT COUNT(*) FROM settings) < ? ON CONFLICT (key) DO NOTHING"
	sqlCompareAndSwapSetting = "UPDATE settings SET value = ? WHERE key = ? AND value = ?"
	sqlGetSetting            = "SELECT value FROM settings WHERE key = ?"
	sqlCountSettings         = "SELECT COUNT(*) FROM settings"
)
