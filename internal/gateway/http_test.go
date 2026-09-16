package gateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/heshwanth/high-performance-api-gateway/internal/config"
	"github.com/heshwanth/high-performance-api-gateway/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

func testConfig(upstream string) config.Config {
	return config.Config{Rate: 1000000, Burst: 1000000, MaxClients: 100, MaxConcurrent: 100, TimeoutSeconds: 2, CacheTTLSeconds: 30, HTTPRoutes: []config.Route{{Prefix: "/api", Upstream: upstream, Cache: true}}}
}

func redisForTest(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr(), MaxRetries: -1, ContextTimeoutEnabled: true})
	t.Cleanup(func() { c.Close() })
	return c, s
}

func request(h http.Handler, path string, headers http.Header) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	if headers != nil {
		r.Header = headers
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCacheHitExpiryAndBypass(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Cache-Control", "public, max-age=30")
		w.Header().Set("Vary", "Accept-Language")
		fmt.Fprintf(w, "%s:%s", r.URL.RequestURI(), r.Header.Get("Accept-Language"))
	}))
	defer upstream.Close()
	rc, redisServer := redisForTest(t)
	c := testConfig(upstream.URL)
	h := NewHTTP(c, rc, ratelimit.New(c.Rate, c.Burst, c.MaxClients), &Stats{})
	first := request(h, "/api/items?x=1", nil)
	second := request(h, "/api/items?x=1", nil)
	if first.Code != 200 || second.Header().Get("X-Cache") != "HIT" || calls.Load() != 1 {
		t.Fatalf("cache failed: calls=%d headers=%v", calls.Load(), second.Header())
	}
	request(h, "/api/items?x=2", nil)
	request(h, "/api/items?x=1", http.Header{"Accept-Language": {"fr"}})
	if calls.Load() != 3 {
		t.Fatal("query/language variants collided")
	}
	for _, name := range []string{"Authorization", "Cookie", "Range", "If-None-Match", "Cache-Control"} {
		request(h, "/api/items?x=1", http.Header{name: {"test"}})
	}
	if calls.Load() != 8 {
		t.Fatal("private/conditional requests used cache")
	}
	redisServer.FastForward(31 * time.Second)
	request(h, "/api/items?x=1", nil)
	if calls.Load() != 9 {
		t.Fatal("expired cache was used")
	}
}

func TestUncacheableResponses(t *testing.T) {
	for _, tc := range []struct{ name, control, vary, cookie string }{
		{"private", "private, max-age=30", "", ""},
		{"no-store", "public, no-store, max-age=30", "", ""},
		{"unspecified", "", "", ""},
		{"vary", "public, max-age=30", "X-Tenant", ""},
		{"cookie", "public, max-age=30", "", "session=abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Cache-Control", tc.control)
				w.Header().Set("Vary", tc.vary)
				if tc.cookie != "" {
					w.Header().Set("Set-Cookie", tc.cookie)
				}
				w.Write([]byte("ok"))
			}))
			defer u.Close()
			rc, _ := redisForTest(t)
			c := testConfig(u.URL)
			h := NewHTTP(c, rc, ratelimit.New(c.Rate, c.Burst, c.MaxClients), &Stats{})
			request(h, "/api/items", nil)
			request(h, "/api/items", nil)
			if calls.Load() != 2 {
				t.Fatal("unsafe response cached")
			}
		})
	}
}

func TestLargeBodyIsNotTruncated(t *testing.T) {
	body := bytes.Repeat([]byte("x"), maxCacheBody+100)
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=30")
		w.(http.Flusher).Flush()
		w.Write(body)
	}))
	defer u.Close()
	rc, _ := redisForTest(t)
	c := testConfig(u.URL)
	h := NewHTTP(c, rc, ratelimit.New(c.Rate, c.Burst, c.MaxClients), &Stats{})
	w := request(h, "/api/large", nil)
	if !bytes.Equal(w.Body.Bytes(), body) {
		t.Fatalf("body has %d bytes, want %d", w.Body.Len(), len(body))
	}
}

func TestRedisFailureStillServesUpstream(t *testing.T) {
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer u.Close()
	rc, _ := redisForTest(t)
	rc.Close()
	c := testConfig(u.URL)
	stats := &Stats{}
	h := NewHTTP(c, rc, ratelimit.New(c.Rate, c.Burst, c.MaxClients), stats)
	if w := request(h, "/api/items", nil); w.Code != 200 || w.Body.String() != "ok" {
		t.Fatalf("response: %v", w)
	}
	if stats.CacheErrors.Load() != 1 {
		t.Fatal("cache error was not counted")
	}
}

func TestRoutingAndForwardedHeaders(t *testing.T) {
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Forwarded-For"); strings.Contains(got, "spoofed") {
			t.Error("trusted spoofed client address")
		}
		fmt.Fprint(w, r.URL.RequestURI())
	}))
	defer u.Close()
	c := testConfig(u.URL)
	h := NewHTTP(c, nil, ratelimit.New(c.Rate, c.Burst, c.MaxClients), &Stats{})
	if w := request(h, "/apix", nil); w.Code != 404 {
		t.Fatal("prefix crossed path boundary")
	}
	if w := request(h, "/api/x?q=1", http.Header{"X-Forwarded-For": {"spoofed"}}); w.Body.String() != "/api/x?q=1" {
		t.Fatal("path/query not preserved")
	}
}

func TestRateLimitAndConcurrency(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-release; w.Write([]byte("ok")) }))
	defer u.Close()
	c := testConfig(u.URL)
	c.MaxConcurrent = 1
	h := NewHTTP(c, nil, ratelimit.New(c.Rate, c.Burst, c.MaxClients), &Stats{})
	done := make(chan struct{})
	go func() { request(h, "/api/x", nil); close(done) }()
	<-started
	w := request(h, "/api/x", nil)
	close(release)
	<-done
	if w.Code != 503 {
		t.Fatalf("overload status=%d", w.Code)
	}
	h = NewHTTP(c, nil, ratelimit.New(0.001, 1, 100), &Stats{})
	request(h, "/unknown", nil)
	if w := request(h, "/unknown", nil); w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatal("rate limit not enforced")
	}
}

func TestRealRedis(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to run against a real Redis server")
	}
	rc := redis.NewClient(&redis.Options{Addr: addr})
	defer rc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rc.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	u := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=1")
		io.WriteString(w, "redis integration")
	}))
	defer u.Close()
	c := testConfig(u.URL)
	h := NewHTTP(c, rc, ratelimit.New(c.Rate, c.Burst, c.MaxClients), &Stats{})
	request(h, "/api/real", nil)
	if w := request(h, "/api/real", nil); w.Header().Get("X-Cache") != "HIT" {
		t.Fatal("real Redis cache miss")
	}
}

func BenchmarkRouteMatch(b *testing.B) {
	for b.Loop() {
		if !matchesPrefix("/api/items/123", "/api") {
			b.Fatal("route did not match")
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// This includes admission, routing, and reverse-proxy work, but excludes sockets,
// Redis, and backend work. It is a routing overhead benchmark, not a client SLA.
func BenchmarkHTTPGatewayNoNetwork(b *testing.B) {
	c := testConfig("http://backend")
	h := newHTTP(c, nil, ratelimit.New(1e12, 1000000, 100), &Stats{}, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), ContentLength: 2, Request: r}, nil
	}))
	b.ResetTimer()
	for b.Loop() {
		w := request(h, "/api/items/42", nil)
		if w.Code != 200 {
			b.Fatal(w.Code)
		}
	}
}

func TestSharedCacheFreshness(t *testing.T) {
	for _, tc := range []struct {
		control string
		want    time.Duration
	}{
		{"public, max-age=30, s-maxage=0", 0},
		{"public, max-age=30, s-maxage=5", 5 * time.Second},
		{"public, max-age=30, max-age=60", 0},
		{"public, max-age=30, private=\"x-user\"", 0},
	} {
		r := &http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": {tc.control}}}
		if got := responseTTL(r, time.Minute); got != tc.want {
			t.Errorf("%s: TTL=%v want=%v", tc.control, got, tc.want)
		}
	}
	r := &http.Response{StatusCode: 200, Header: http.Header{"Cache-Control": {"public, max-age=30"}, "Date": {time.Now().Add(-20 * time.Second).UTC().Format(http.TimeFormat)}}}
	if got := responseTTL(r, time.Minute); got < 8*time.Second || got > 10*time.Second {
		t.Fatalf("upstream age ignored: %v", got)
	}
}
