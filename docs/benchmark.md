# Local benchmark

Measured on September 16, 2026, on an Apple M2 Pro (darwin/arm64), using Go 1.27.1. The gateway, Redis 7.4, and demo backend ran in Docker Desktop. The HTTP load generator ran on the host with 16 workers. The demo backend adds 5 ms of work to every HTTP call.

```sh
docker compose up --build -d --wait
go run ./cmd/loadtest -n 1000 -unique 600 -concurrency 16
go test ./internal/ratelimit ./internal/gateway -run '^$' -bench . -benchmem
```

The HTTP workload has 600 unique items followed by 400 repeats, within the cache TTL. Both the bypass and cached runs sent 1,000 requests. All 2,000 returned HTTP 200. Redis caching reduced backend calls from 1,000 to 600, giving a measured 40% reduction for this deliberately chosen request mix. This does not estimate the hit rate of an unknown production workload.

The raw HTTP output is in [loadtest.json](loadtest.json). Its percentiles include networking, Redis, and the demo service's delay. The mixed cached run includes cold requests, so its median is not the latency of a cache hit alone.

| Microbenchmark | Time per operation | Bytes per operation | Allocations |
| --- | ---: | ---: | ---: |
| Token bucket, concurrent callers | 208.5 ns | 0 | 0 |
| Path-prefix match | 13.71 ns | 0 | 0 |
| HTTP gateway with in-memory upstream | 15,885 ns (0.0159 ms) | 40,635 | 39 |

The full HTTP microbenchmark includes request/recorder construction, token-bucket admission, concurrency admission, path selection, timeout context creation, and reverse-proxy handling. Its upstream transport returns a two-byte response in memory; there is no socket, Redis call, or backend service work. It establishes sub-millisecond routing overhead under those conditions, not sub-millisecond end-to-end latency. It runs without the race detector; correctness tests run separately with it.

The relatively large allocation count includes the reverse proxy's copy buffer and the test HTTP request/response objects. The benchmark is useful as a baseline for future buffer-pool or allocation work, without claiming those costs have already been optimized.

For a resume, a precise description is: “Built a concurrent HTTP/gRPC gateway in Go with token-bucket rate limiting and Redis caching; reduced downstream calls by 40% in a repeat-request benchmark and measured 0.016 ms in-memory HTTP routing overhead.”
