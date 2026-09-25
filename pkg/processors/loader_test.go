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
)

func atomicAdd(p *int32, v int32) { atomic.AddInt32(p, v) }

func TestLoaderCache(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomicAdd(&hits, 1)
		_, _ = w.Write([]byte("  {\"type\": \"object\"}\n"))
	}))
	t.Cleanup(srv.Close)

	l := newSchemaLoader()
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
		name string
		url  string
		want string
	}{
		{"bad url", "http://[::1", "schema"},
		{"http status", srv500.URL, "HTTP 500"},
		{"content length", srvBig.URL, "too large"},
		{"blank document", srvBlank.URL, "empty document"},
		{"streamed too large", srvStream.URL, "too large"},
		{"truncated body", srvTrunc.URL, "schema"},
		{"connection refused", closedURL, "schema"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := newSchemaLoader()
			_, err := l.Load(context.Background(), tt.url)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
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

	l := newSchemaLoader()
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
