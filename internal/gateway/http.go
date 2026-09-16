package gateway

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/heshwanth/high-performance-api-gateway/internal/config"
	"github.com/heshwanth/high-performance-api-gateway/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

type httpRoute struct {
	prefix string
	proxy  *httputil.ReverseProxy
}

func ClientIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func NewHTTP(c config.Config, cache *redis.Client, limiter *ratelimit.Limiter, stats *Stats) http.Handler {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 1024
	transport.MaxIdleConnsPerHost = 256
	transport.MaxConnsPerHost = c.MaxConcurrent
	transport.ResponseHeaderTimeout = time.Duration(c.TimeoutSeconds) * time.Second
	transport.DisableCompression = true
	return newHTTP(c, cache, limiter, stats, transport)
}

func newHTTP(c config.Config, cache *redis.Client, limiter *ratelimit.Limiter, stats *Stats, transport http.RoundTripper) http.Handler {
	routes := make([]httpRoute, 0, len(c.HTTPRoutes))
	for _, route := range c.HTTPRoutes {
		target, _ := url.Parse(route.Upstream)
		var rc *redis.Client
		if route.Cache {
			rc = cache
		}
		proxy := &httputil.ReverseProxy{
			Rewrite:   func(r *httputil.ProxyRequest) { r.SetURL(target); r.SetXForwarded() },
			Transport: &cacheTransport{base: transport, redis: rc, ttl: time.Duration(c.CacheTTLSeconds) * time.Second, stats: stats},
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				slog.Warn("upstream request failed", "path", r.URL.Path, "error", err)
				code := http.StatusBadGateway
				if r.Context().Err() != nil {
					code = http.StatusGatewayTimeout
				}
				http.Error(w, http.StatusText(code), code)
			},
		}
		routes = append(routes, httpRoute{prefix: route.Prefix, proxy: proxy})
	}
	sort.Slice(routes, func(i, j int) bool { return len(routes[i].prefix) > len(routes[j].prefix) })
	slots := make(chan struct{}, c.MaxConcurrent)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.Write([]byte("ok\n"))
			return
		}
		if r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(stats.Snapshot())
			return
		}
		stats.Requests.Add(1)
		if ok, retry := limiter.Allow(ClientIP(r.RemoteAddr), time.Now()); !ok {
			stats.RateLimited.Add(1)
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			stats.Overloaded.Add(1)
			http.Error(w, "gateway busy", http.StatusServiceUnavailable)
			return
		}
		for _, route := range routes {
			if matchesPrefix(r.URL.Path, route.prefix) {
				ctx, cancel := context.WithTimeout(r.Context(), time.Duration(c.TimeoutSeconds)*time.Second)
				defer cancel()
				route.proxy.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}
		http.NotFound(w, r)
	})
}

func matchesPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, strings.TrimSuffix(prefix, "/")+"/")
}
