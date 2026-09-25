package processors

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// maxSchemaBytes caps the size of a fetched JSON Schema document.
const maxSchemaBytes = 1 << 20 // 1 MiB

// schemaLoader fetches JSON Schema documents over HTTP and caches them by
// URL for a configurable duration (ttl): within its lifetime a schema is
// served from cache, afterwards it is re-fetched. A ttl <= 0 disables the
// cache entirely (a schema is fetched for every message).
type schemaLoader struct {
	client *http.Client
	ttl    time.Duration
	now    func() time.Time

	mu    sync.Mutex
	cache map[string]cacheEntry
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
		l.mu.Unlock()
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

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("schema %s: HTTP %d", url, resp.StatusCode)
	}
	if resp.ContentLength > maxSchemaBytes {
		return "", fmt.Errorf("schema %s: document too large", url)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxSchemaBytes+1))
	if err != nil {
		return "", fmt.Errorf("schema %s: %w", url, err)
	}
	if len(body) > maxSchemaBytes {
		return "", fmt.Errorf("schema %s: document too large", url)
	}

	doc := strings.TrimSpace(string(body))
	if doc == "" {
		return "", fmt.Errorf("schema %s: empty document", url)
	}

	if l.ttl > 0 {
		l.mu.Lock()
		l.cache[url] = cacheEntry{doc: doc, expiresAt: l.now().Add(l.ttl)}
		l.mu.Unlock()
	}
	return doc, nil
}
