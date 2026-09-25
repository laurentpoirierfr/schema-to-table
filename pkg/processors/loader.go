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
// URL, so a schema shared across a stream is network-fetched (and later
// re-parsed by the normalized planner) only once.
type schemaLoader struct {
	client *http.Client
	mu     sync.Mutex
	cache  map[string]string
}

func newSchemaLoader() *schemaLoader {
	return &schemaLoader{
		client: &http.Client{Timeout: 30 * time.Second},
		cache:  map[string]string{},
	}
}

// Load returns the schema document for url, reusing a previously fetched
// copy when available.
func (l *schemaLoader) Load(ctx context.Context, url string) (string, error) {
	l.mu.Lock()
	doc, ok := l.cache[url]
	l.mu.Unlock()
	if ok {
		return doc, nil
	}

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

	doc = strings.TrimSpace(string(body))
	if doc == "" {
		return "", fmt.Errorf("schema %s: empty document", url)
	}

	l.mu.Lock()
	l.cache[url] = doc
	l.mu.Unlock()
	return doc, nil
}
