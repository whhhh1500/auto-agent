package modelsettings

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultRepositoryCacheTTL = 5 * time.Second

// CachedRepository adds a small, synchronous read-through cache to a model
// settings repository. It has no background refresh: an expired value is
// never returned until the authoritative repository has been read again.
type CachedRepository struct {
	base       Repository
	ttl        time.Duration
	now        func() time.Time
	mu         sync.Mutex
	mutationMu sync.Mutex
	entry      cacheEntry
	load       chan struct{}
	epoch      uint64
}

type cacheEntry struct {
	value   StoredConfiguration
	found   bool
	valid   bool
	expires time.Time
}

// AbsentCreator is the optional atomic create-if-absent seam. CachedRepository
// never implements this operation using its cache; it always delegates the
// atomic decision to the underlying repository.
type AbsentCreator interface {
	CreateIfAbsent(context.Context, StoredConfiguration) (bool, error)
}

var _ Repository = (*CachedRepository)(nil)

func NewCachedRepository(base Repository, clock func() time.Time) (*CachedRepository, error) {
	if base == nil {
		return nil, fmt.Errorf("model settings cache requires a repository")
	}
	if clock == nil {
		clock = time.Now
	}
	return &CachedRepository{base: base, ttl: defaultRepositoryCacheTTL, now: clock}, nil
}

func (c *CachedRepository) Load(ctx context.Context) (StoredConfiguration, bool, error) {
	if c == nil || c.base == nil {
		return StoredConfiguration{}, false, errors.New("model settings cache is unavailable")
	}
	if ctx == nil {
		return StoredConfiguration{}, false, errors.New("model settings cache requires a context")
	}
	for {
		now := c.now()
		c.mu.Lock()
		if c.entry.valid && now.Before(c.entry.expires) {
			value, found := cloneCached(c.entry.value), c.entry.found
			c.mu.Unlock()
			return value, found, nil
		}
		if c.load != nil {
			wait := c.load
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return StoredConfiguration{}, false, ctx.Err()
			case <-wait:
				continue
			}
		}
		wait := make(chan struct{})
		c.load = wait
		loadEpoch := c.epoch
		c.mu.Unlock()

		value, found, err := c.base.Load(ctx)
		if err == nil && found {
			value, err = NormalizeStoredConfiguration(value)
		}
		c.mu.Lock()
		c.load = nil
		changed := c.epoch != loadEpoch
		if err == nil && !changed {
			c.entry = cacheEntry{value: cloneCached(value), found: found, valid: true, expires: c.now().Add(c.ttl)}
		} else if err != nil && !changed {
			c.entry = cacheEntry{}
		}
		close(wait)
		c.mu.Unlock()
		if changed {
			continue
		}
		if err != nil {
			return StoredConfiguration{}, false, err
		}
		return cloneCached(value), found, nil
	}
}

func (c *CachedRepository) Save(ctx context.Context, value StoredConfiguration) error {
	if c == nil || c.base == nil {
		return errors.New("model settings cache is unavailable")
	}
	if ctx == nil {
		return errors.New("model settings cache requires a context")
	}
	copyOf, err := NormalizeStoredConfiguration(cloneCached(value))
	if err != nil {
		return err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if err := c.base.Save(ctx, cloneCached(copyOf)); err != nil {
		c.invalidate()
		return err
	}
	c.mu.Lock()
	c.epoch++
	c.entry = cacheEntry{value: cloneCached(copyOf), found: true, valid: true, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return nil
}

// CreateIfAbsent delegates directly to the optional atomic repository seam.
// A failed/non-created operation invalidates the cache so the next Load sees
// the winner's durable value.
func (c *CachedRepository) CreateIfAbsent(ctx context.Context, value StoredConfiguration) (bool, error) {
	if c == nil || c.base == nil {
		return false, errors.New("model settings cache is unavailable")
	}
	if ctx == nil {
		return false, errors.New("model settings cache requires a context")
	}
	creator, ok := c.base.(AbsentCreator)
	if !ok {
		return false, errors.New("model settings repository does not support atomic create-if-absent")
	}
	copyOf, err := NormalizeStoredConfiguration(cloneCached(value))
	if err != nil {
		return false, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	created, err := creator.CreateIfAbsent(ctx, cloneCached(copyOf))
	if err != nil || !created {
		c.invalidate()
		return created, err
	}
	c.mu.Lock()
	c.epoch++
	c.entry = cacheEntry{value: cloneCached(copyOf), found: true, valid: true, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return true, nil
}

func (c *CachedRepository) invalidate() {
	c.mu.Lock()
	c.epoch++
	c.entry = cacheEntry{}
	c.mu.Unlock()
}

func cloneCached(value StoredConfiguration) StoredConfiguration {
	value.AllowedModels = append([]string(nil), value.AllowedModels...)
	return value
}
