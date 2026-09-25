package processors

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

// maxSchemaBytes caps the size of a fetched JSON Schema document.
const maxSchemaBytes = 1 << 20 // 1 MiB

// schemaLoader fetches JSON Schema documents over HTTP and caches them by
// URL for a configurable duration (ttl): within its lifetime a schema is
// served from cache, afterwards it is re-fetched. A ttl <= 0 disables the
// cache entirely (a schema is fetched for every message).
//
// The cache is bounded in time, not in size: expired entries are purged on a
// lazy sweep (at most once per ttl) so URLs seen once do not accumulate.
type schemaLoader struct {
	client *http.Client
	ttl    time.Duration
	now    func() time.Time

	cacheHits *service.MetricCounter // non-nil when a Metrics is wired
	fetches   *service.MetricCounter

	mu        sync.Mutex
	cache     map[string]cacheEntry
	lastSweep time.Time
}

type cacheEntry struct {
	doc       string
	expiresAt time.Time
}

func newSchemaLoader(ttl time.Duration) *schemaLoader {
	return &schemaLoader{
		client: &http.Client{Timeout: 30 * time.Second},
		ttl:    ttl,
		now:    time.Now,
		cache:  map[string]cacheEntry{},
	}
}

// Load returns the schema document for url, reusing a previously fetched
// copy while it has not expired.
func (l *schemaLoader) Load(ctx context.Context, url string) (string, error) {
	l.mu.Lock()
	if entry, ok := l.cache[url]; ok && l.now().Before(entry.expiresAt) {
		l.sweepLocked()
		l.mu.Unlock()
		l.cacheHits.Incr(1)
		return entry.doc, nil
	}
	l.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("schema %s: %w", url, err)
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("schema %s: %w", url, err)
	}
	defer resp.Body.Close()
	l.fetches.Incr(1)

	// 5xx and transport errors are transient (the whole batch should be
	// retried later); 4xx, oversized and empty documents are permanent.
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("schema %s: HTTP %d", url, resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return "", permanent(err)
		}
		return "", err
	}
	if resp.ContentLength > maxSchemaBytes {
		return "", permanent(fmt.Errorf("schema %s: document too large", url))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSchemaBytes+1))
	if err != nil {
		return "", fmt.Errorf("schema %s: %w", url, err)
	}
	if len(body) > maxSchemaBytes {
		return "", permanent(fmt.Errorf("schema %s: document too large", url))
	}

	doc := strings.TrimSpace(string(body))
	if doc == "" {
		return "", permanent(fmt.Errorf("schema %s: empty document", url))
	}

	if l.ttl > 0 {
		l.mu.Lock()
		l.cache[url] = cacheEntry{doc: doc, expiresAt: l.now().Add(l.ttl)}
		l.sweepLocked()
		l.mu.Unlock()
	}
	return doc, nil
}

// sweepLocked drops expired cache entries. It runs at most once per ttl so
// an idle-processor lookups stay cheap; with no caching (ttl <= 0) it never
// runs.
func (l *schemaLoader) sweepLocked() {
	if l.ttl <= 0 {
		return
	}
	now := l.now()
	if now.Sub(l.lastSweep) < l.ttl {
		return
	}
	for u, e := range l.cache {
		if !now.Before(e.expiresAt) {
			delete(l.cache, u)
		}
	}
	l.lastSweep = now
}
