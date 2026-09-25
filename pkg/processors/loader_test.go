package processors

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func atomicAdd(p *int32, v int32) { atomic.AddInt32(p, v) }

func TestLoaderCache(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomicAdd(&hits, 1)
		_, _ = w.Write([]byte("  {\"type\": \"object\"}\n"))
	}))
	t.Cleanup(srv.Close)

	l := newSchemaLoader(time.Hour)
	ctx := context.Background()
	doc1, err := l.Load(ctx, srv.URL)
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	doc2, err := l.Load(ctx, srv.URL)
	if err != nil {
		t.Fatalf("second Load: %v", err)
	}
	if doc1 != doc2 {
		t.Errorf("cached document differs: %q vs %q", doc1, doc2)
	}
	if doc1 != `{"type": "object"}` {
		t.Errorf("document not trimmed to content: %q", doc1)
	}
	if hits != 1 {
		t.Errorf("schema fetched %d times, want 1", hits)
	}
}

func TestLoaderErrors(t *testing.T) {
	fail := func(status int, body string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status != 0 {
				w.WriteHeader(status)
				return
			}
			_, _ = w.Write([]byte(body))
		}))
	}

	// HTTP 500 hides the document.
	srv500 := fail(http.StatusInternalServerError, "")
	t.Cleanup(srv500.Close)

	// HTTP 404: a permanently missing schema.
	srv404 := fail(http.StatusNotFound, "")
	t.Cleanup(srv404.Close)

	// Content-Length announced above the cap without a matching body: the
	// loader must reject it from the header alone.
	srvBig := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(maxSchemaBytes+1))
	}))
	t.Cleanup(srvBig.Close)

	// Zero-length body (whitespace only).
	srvBlank := fail(0, " \n  ")
	t.Cleanup(srvBlank.Close)

	// Body streamed beyond the cap without a Content-Length header.
	srvStream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i <= maxSchemaBytes/4096; i++ {
			_, _ = w.Write(make([]byte, 4096))
		}
	}))
	t.Cleanup(srvStream.Close)

	// Body announced longer than the stream actually is: reading fails with
	// a client-side error (unexpected EOF).
	srvTrunc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("tiny"))
	}))
	t.Cleanup(srvTrunc.Close)

	// A port that is guaranteed closed: the connection itself fails.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	closedURL := "http://" + ln.Addr().String() + "/schema.json"
	_ = ln.Close()

	for _, tt := range []struct {
		name      string
		url       string
		want      string
		permanent bool
	}{
		{"bad url", "http://[::1", "schema", false},
		{"http status", srv500.URL, "HTTP 500", false},
		{"http 404 schema", srv404.URL, "HTTP 404", true},
		{"content length", srvBig.URL, "too large", true},
		{"blank document", srvBlank.URL, "empty document", true},
		{"streamed too large", srvStream.URL, "too large", true},
		{"truncated body", srvTrunc.URL, "schema", false},
		{"connection refused", closedURL, "schema", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newSchemaLoader(time.Hour)
			_, err := l.Load(context.Background(), tt.url)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
			if isPermanent(err) != tt.permanent {
				t.Errorf("isPermanent(%v) = %v, want %v", err, isPermanent(err), tt.permanent)
			}
		})
	}
}

// TestLoaderConcurrent hammers the same URL from many goroutines: the cache
// stays consistent (the trimmed document is always returned, network hits are
// bounded by the number of concurrent misses) and the mutex holds.
func TestLoaderConcurrent(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomicAdd(&hits, 1)
		_, _ = w.Write([]byte("  {\"type\": \"object\"}"))
	}))
	t.Cleanup(srv.Close)

	l := newSchemaLoader(time.Hour)
	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			doc, err := l.Load(context.Background(), srv.URL)
			if err != nil {
				errs <- err
				return
			}
			if doc != `{"type": "object"}` {
				errs <- fmt.Errorf("unexpected cached doc: %q", doc)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestLoaderTTLRefreshes advances the injected clock past the TTL: the
// cached document expires and the schema is fetched again.
func TestLoaderTTLRefreshes(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomicAdd(&hits, 1)
		_, _ = w.Write([]byte("{\"type\": \"object\"}"))
	}))
	t.Cleanup(srv.Close)

	ttl := 10 * time.Minute
	now := time.Now()
	l := newSchemaLoader(ttl)
	l.now = func() time.Time { return now }

	ctx := context.Background()
	doc1, err := l.Load(ctx, srv.URL)
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	if _, err := l.Load(ctx, srv.URL); err != nil {
		t.Fatalf("second Load: %v", err)
	}
	if hits != 1 {
		t.Fatalf("schema fetched %d times within TTL, want 1", hits)
	}

	// Advance past the TTL: the next Load must re-fetch.
	now = now.Add(ttl + time.Second)
	doc2, err := l.Load(ctx, srv.URL)
	if err != nil {
		t.Fatalf("Load after expiry: %v", err)
	}
	if doc2 != doc1 {
		t.Errorf("re-fetched document differs: %q vs %q", doc2, doc1)
	}
	if hits != 2 {
		t.Errorf("schema fetched %d times after expiry, want 2", hits)
	}

	// Within the new TTL window the cache serves the document again.
	if _, err := l.Load(ctx, srv.URL); err != nil {
		t.Fatalf("Load back within TTL: %v", err)
	}
	if hits != 2 {
		t.Errorf("schema fetched %d times back within TTL, want 2", hits)
	}
}

// TestLoaderTTLDisabled with ttl <= 0 never caches: every Load fetches.
func TestLoaderTTLDisabled(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomicAdd(&hits, 1)
		_, _ = w.Write([]byte("{\"type\": \"object\"}"))
	}))
	t.Cleanup(srv.Close)

	l := newSchemaLoader(0)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		doc, err := l.Load(ctx, srv.URL)
		if err != nil {
			t.Fatalf("Load %d: %v", i, err)
		}
		if doc != `{"type": "object"}` {
			t.Errorf("doc %d = %q", i, doc)
		}
	}
	if hits != 2 {
		t.Errorf("schema fetched %d times with cache disabled, want 2", hits)
	}
}

// TestLoaderSweepRemovesExpired advances the injected clock: expired entries
// are purged on the lazy sweep, unvisited URLs do not accumulate.
func TestLoaderSweepRemovesExpired(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("{\"type\": \"object\"}"))
	}))
	t.Cleanup(srv.Close)

	ttl := time.Minute
	now := time.Now()
	l := newSchemaLoader(ttl)
	l.now = func() time.Time { return now }

	ctx := context.Background()
	if _, err := l.Load(ctx, srv.URL+"/a"); err != nil {
		t.Fatalf("load a: %v", err)
	}
	now = now.Add(30 * time.Second)
	if _, err := l.Load(ctx, srv.URL+"/b"); err != nil {
		t.Fatalf("load b: %v", err)
	}

	l.mu.Lock()
	if got := len(l.cache); got != 2 {
		l.mu.Unlock()
		t.Fatalf("cache has %d entries before sweep, want 2", got)
	}
	l.mu.Unlock()

	// Advance well past both expiries and trigger a sweep by loading a third
	// URL: the sweep drops a and b.
	now = now.Add(2 * time.Minute)
	if _, err := l.Load(ctx, srv.URL+"/c"); err != nil {
		t.Fatalf("load c: %v", err)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if got := len(l.cache); got != 1 {
		t.Fatalf("cache has %d entries after sweep, want 1 (c only)", got)
	}
	if _, ok := l.cache[srv.URL+"/c"]; !ok {
		t.Errorf("entry c missing after sweep")
	}
}
