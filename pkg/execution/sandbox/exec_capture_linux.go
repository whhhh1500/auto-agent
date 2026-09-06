//go:build linux

package sandbox

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"sync"
)

type boundedCapture struct {
	mu       sync.Mutex
	preview  []byte
	digest   hash.Hash
	size     int64
	limit    int64
	combined *combinedCapture
	exceeded bool
}

type combinedCapture struct {
	mu           sync.Mutex
	total, limit int64
	kill         func()
	killOnce     sync.Once
}

func newBoundedCapture(combined *combinedCapture, limit int64) *boundedCapture {
	return &boundedCapture{digest: sha256.New(), limit: limit, combined: combined}
}

func (c *combinedCapture) add(n int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n < 0 || n > c.limit-c.total {
		c.total = c.limit
		if c.kill != nil {
			c.killOnce.Do(c.kill)
		}
		return false
	}
	c.total += n
	return true
}

func (c *boundedCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.exceeded {
		c.mu.Unlock()
		return 0, ErrExecOutputLimit
	}
	c.mu.Unlock()
	if c.combined != nil && !c.combined.add(int64(len(p))) {
		c.mu.Lock()
		c.exceeded = true
		c.mu.Unlock()
		return 0, ErrExecOutputLimit
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.digest == nil || int64(len(p)) > c.limit-c.size {
		c.exceeded = true
		return 0, ErrExecOutputLimit
	}
	c.size += int64(len(p))
	_, _ = c.digest.Write(p)
	if int64(len(c.preview)) < DefaultExecPreviewBytes {
		n := int(DefaultExecPreviewBytes - int64(len(c.preview)))
		if n > len(p) {
			n = len(p)
		}
		c.preview = append(c.preview, p[:n]...)
	}
	return len(p), nil
}

func (c *boundedCapture) result() Output {
	c.mu.Lock()
	defer c.mu.Unlock()
	sum := c.digest.Sum(nil)
	return Output{Preview: append([]byte(nil), c.preview...), Size: c.size, Digest: hex.EncodeToString(sum), Truncated: c.size > int64(len(c.preview))}
}

func (c *boundedCapture) exceededLimit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.exceeded
}

var _ io.Writer = (*boundedCapture)(nil)
