package gateway

import "sync/atomic"

type Stats struct {
	Requests         atomic.Int64
	CacheHits        atomic.Int64
	CacheMisses      atomic.Int64
	CacheErrors      atomic.Int64
	UpstreamRequests atomic.Int64
	RateLimited      atomic.Int64
	Overloaded       atomic.Int64
	GRPCStreams      atomic.Int64
}

func (s *Stats) Snapshot() map[string]int64 {
	return map[string]int64{
		"http_requests": s.Requests.Load(), "cache_hits": s.CacheHits.Load(),
		"cache_misses": s.CacheMisses.Load(), "cache_errors": s.CacheErrors.Load(),
		"http_upstream_requests": s.UpstreamRequests.Load(), "rate_limited": s.RateLimited.Load(),
		"overloaded": s.Overloaded.Load(), "grpc_streams": s.GRPCStreams.Load(),
	}
}
