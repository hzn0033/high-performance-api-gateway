package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxCacheBody = 1 << 20

type cachedResponse struct {
	Header  http.Header `json:"header"`
	Body    []byte      `json:"body"`
	Stored  time.Time   `json:"stored"`
	Expires time.Time   `json:"expires"`
}

type cacheTransport struct {
	base  http.RoundTripper
	redis *redis.Client
	ttl   time.Duration
	stats *Stats
}

func cacheableRequest(r *http.Request) bool {
	if r.Method != http.MethodGet || r.Body != nil && r.Body != http.NoBody {
		return false
	}
	for _, h := range []string{"Authorization", "Cookie", "Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since", "If-Range", "Cache-Control", "Pragma"} {
		if len(r.Header.Values(h)) > 0 {
			return false
		}
	}
	return true
}

func cacheKey(r *http.Request) string {
	// Vary is restricted to these headers when storing a response.
	data, _ := json.Marshal([]any{r.URL.String(), r.Host, r.Header.Values("Accept"), r.Header.Values("Accept-Encoding"), r.Header.Values("Accept-Language")})
	sum := sha256.Sum256(data)
	return "gateway:v1:" + hex.EncodeToString(sum[:])
}

func responseTTL(r *http.Response, ceiling time.Duration) time.Duration {
	if r.StatusCode != http.StatusOK || len(r.Header.Values("Set-Cookie")) > 0 || len(r.Trailer) > 0 || r.ContentLength > maxCacheBody {
		return 0
	}
	for _, field := range strings.Split(strings.Join(r.Header.Values("Vary"), ","), ",") {
		switch strings.ToLower(strings.TrimSpace(field)) {
		case "", "accept", "accept-encoding", "accept-language":
		default:
			return 0
		}
	}
	public := false
	ttl := time.Duration(0)
	sharedTTL := time.Duration(-1)
	seen := make(map[string]bool)
	for _, directive := range strings.Split(strings.Join(r.Header.Values("Cache-Control"), ","), ",") {
		parts := strings.SplitN(strings.ToLower(strings.TrimSpace(directive)), "=", 2)
		switch parts[0] {
		case "private", "no-store", "no-cache":
			return 0
		case "public":
			public = true
		case "max-age", "s-maxage":
			if len(parts) != 2 || seen[parts[0]] {
				return 0
			}
			seen[parts[0]] = true
			if len(parts) == 2 {
				seconds, err := strconv.ParseInt(strings.Trim(parts[1], "\""), 10, 32)
				if err != nil || seconds < 0 {
					return 0
				}
				if parts[0] == "s-maxage" {
					sharedTTL = time.Duration(seconds) * time.Second
				} else {
					ttl = time.Duration(seconds) * time.Second
				}
			}
		}
	}
	if !public {
		return 0
	}
	if sharedTTL >= 0 {
		ttl = sharedTTL
	}
	// Respect time already spent in an upstream cache.
	ttl -= responseAge(r.Header)
	if ttl > ceiling {
		ttl = ceiling
	}
	return ttl
}

func responseAge(h http.Header) time.Duration {
	age := time.Duration(0)
	if seconds, err := strconv.ParseInt(h.Get("Age"), 10, 32); err == nil && seconds > 0 {
		age = time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(h.Get("Date")); err == nil {
		if apparent := time.Since(date); apparent > age {
			age = apparent
		}
	}
	return age
}

func (t *cacheTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.redis == nil || !cacheableRequest(r) {
		return t.fetch(r)
	}
	key := cacheKey(r)
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Millisecond)
	data, err := t.redis.Get(ctx, key).Bytes()
	cancel()
	if err == nil {
		var entry cachedResponse
		if json.Unmarshal(data, &entry) == nil && entry.Header != nil && time.Now().Before(entry.Expires) {
			t.stats.CacheHits.Add(1)
			entry.Header.Set("X-Cache", "HIT")
			age, _ := strconv.Atoi(entry.Header.Get("Age"))
			entry.Header.Set("Age", strconv.Itoa(age+int(time.Since(entry.Stored).Seconds())))
			return &http.Response{StatusCode: 200, Header: entry.Header, Body: io.NopCloser(bytes.NewReader(entry.Body)), ContentLength: int64(len(entry.Body)), Request: r}, nil
		}
	} else if err != redis.Nil {
		t.stats.CacheErrors.Add(1)
	}
	t.stats.CacheMisses.Add(1)
	resp, err := t.fetch(r)
	if err != nil {
		return nil, err
	}
	resp.Header.Set("X-Cache", "MISS")
	ttl := responseTTL(resp, t.ttl)
	if ttl <= 0 {
		return resp, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxCacheBody+1))
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	if len(body) > maxCacheBody {
		// Keep streaming large responses instead of truncating them to the cache limit.
		resp.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(body), resp.Body), Closer: resp.Body}
		return resp, nil
	}
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	// Reading a slow body consumes freshness too.
	ttl = responseTTL(resp, t.ttl)
	if ttl <= 0 {
		return resp, nil
	}
	now := time.Now()
	entry := cachedResponse{Header: resp.Header.Clone(), Body: body, Stored: now, Expires: now.Add(ttl)}
	entry.Header.Set("Age", strconv.Itoa(int(responseAge(resp.Header).Seconds())))
	data, err = json.Marshal(entry)
	if err == nil {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Millisecond)
		err = t.redis.Set(ctx, key, data, ttl).Err()
		cancel()
		if err != nil {
			t.stats.CacheErrors.Add(1)
		}
	}
	return resp, nil
}

type prefixedBody struct {
	io.Reader
	io.Closer
}

func (t *cacheTransport) fetch(r *http.Request) (*http.Response, error) {
	t.stats.UpstreamRequests.Add(1)
	return t.base.RoundTrip(r)
}
