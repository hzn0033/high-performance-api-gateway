# High-performance API gateway

A small Go gateway that routes HTTP and gRPC traffic, limits requests with token buckets, and caches public HTTP responses in Redis. It includes a demo backend, a Docker Compose setup, and a load test that counts how many requests actually reach the backend.

The code is split by responsibility: HTTP forwarding, gRPC streams, caching, and rate limiting. There is no framework around the standard HTTP reverse proxy.

```text
HTTP clients ── :8080 ── rate limit ── concurrency limit ── Redis cache
                                                           │ miss
                                                           ▼
                                                      HTTP backend

gRPC clients ── :9090 ── rate limit ── concurrency limit ── gRPC backend
```

## Run it

With Docker Desktop or Docker Engine and Compose installed:

```sh
docker compose up --build -d --wait

curl -i http://localhost:8080/api/items/42
curl -i http://localhost:8080/api/items/42
docker compose exec gateway /app/grpc-check
curl http://localhost:8080/metrics
```

The first HTTP response has `X-Cache: MISS`; the second has `X-Cache: HIT`. The gRPC command prints `SERVING` after calling the demo health service through the gateway.

```sh
docker compose logs -f gateway
docker compose down
```

Compose exposes only the gateway, bound to localhost. Redis and the demo service stay on the internal Docker network. Redis uses a 64 MB eviction limit and does not persist this disposable cache.

For local Go development, install Go 1.26 or newer and run Redis on port 6379. Start these in separate terminals:

```sh
go run ./cmd/demo
go run ./cmd/gateway -config configs/local.json
```

## Routes and limits

Edit `configs/local.json` for local development or `configs/docker.json` for Compose. Restart the gateway after changing the configuration.

| Setting | Default | Meaning |
| --- | --- | --- |
| `rate_per_second` | 1,000 | Tokens added per client per second |
| `burst` | 2,000 | Maximum tokens per client |
| `max_clients` | 10,000 | Maximum tracked client buckets |
| `max_concurrent` | 1,024 | In-flight HTTP requests and, separately, gRPC streams |
| `timeout_seconds` | 10 | Upstream request/stream lifetime limit |
| `cache_ttl_seconds` | 30 | Upper bound on cache freshness |

HTTP routes use the longest matching path prefix, with a path boundary: `/api` matches `/api/items`, but not `/apix`. The full path and query are forwarded. An upstream URL may add a base path; route prefixes are not stripped.

gRPC routes map the full protobuf service name to an upstream address. For example, `grpc.health.v1.Health` forwards both `Check` and `Watch`. The gateway does not need the backend's generated protobuf files. It forwards protobuf frames, metadata, trailers, and status codes, including bidirectional streams and client half-close. Individual messages are limited to 4 MB.

HTTP and gRPC share each client's token bucket. Client identity comes from the socket's remote IP; forwarded headers are not trusted for rate limiting. Buckets refill lazily, and fully replenished buckets are removed during periodic request-driven sweeps. Exhausting the client table denies new clients until space is reclaimed. Limits are per gateway process; multiple replicas do **not** share a global quota. gRPC consumes one token per RPC, not per streamed message.

HTTP exhaustion returns `429` with `Retry-After`. Concurrency exhaustion returns `503`. gRPC uses `RESOURCE_EXHAUSTED` for either condition. Buffered channels cap admitted work without building an unbounded request queue.

## Cache behavior

Caching must be enabled on a route. Only anonymous, bodyless `GET` requests whose upstream returns `200` and an explicit `public` freshness policy are stored. Responses expire at the earlier of the configured TTL and their remaining `max-age`/`s-maxage`. Upstream `Age` and `Date` are taken into account.

- Requests with authorization, cookies, range/conditional headers, or cache-control directives bypass Redis.
- Responses with `Set-Cookie`, `private`, `no-store`, `no-cache`, trailers, or unsupported `Vary` fields are skipped.
- The cache key includes the upstream URL, query, host, and `Accept`, `Accept-Encoding`, and `Accept-Language` variants.
- Bodies larger than 1 MB keep streaming and are not cached.
- Redis reads and writes have a 20 ms context budget. Cache failures fall through to the backend and increment `cache_errors`; they can still add latency.

`REDIS_ADDR` overrides the configuration address. `REDIS_PASSWORD` is read from the environment. Do not commit credentials.

There is no cache invalidation API or request coalescing. Simultaneous cold requests can each reach the backend, and changed data may remain visible until its TTL expires. This cache is intended for explicitly public, short-lived responses.

## Measure it

With the Compose stack running and Go installed:

```sh
go run ./cmd/loadtest -n 1000 -unique 600 -concurrency 16
make bench
```

The load test runs 1,000 requests with caching bypassed, then the same mix with caching enabled. Each run has 600 distinct items followed by 400 repeat visits. A fresh query namespace prevents previous runs from warming this one. It reports successful requests, status counts, gateway upstream-request counters, cache hits, throughput, and end-to-end p50/p95/p99 latency. Run it against an otherwise idle gateway so the counter differences are meaningful.

A fully successful run with 400 hits reduces downstream calls from 1,000 to 600: **40% for this particular workload**. This result depends on repetition and TTL, and is not a general promise about every API. Increasing concurrency can also cause rate limiting; failed runs exit nonzero and are marked invalid in the output.

`make bench` measures token-bucket contention, path matching, and the full HTTP gateway with an in-memory upstream. Those timings do not establish sub-millisecond end-to-end request latency. Redis, backend work, network hops, and queueing all count toward what a client sees. See [the recorded local run](docs/benchmark.md) for measured results and conditions.

## Tests

```sh
make test
# Optional: also exercise a real Redis instance.
TEST_REDIS_ADDR=localhost:6379 go test -race ./internal/gateway
```

The ordinary suite starts an in-process Redis test server. Tests cover concurrent token consumption, refill and eviction, cache expiry and private-response bypass, query/header isolation, large bodies, Redis failures, route boundaries, overload rejection, and gRPC unary/streaming behavior, metadata, half-close, cancellation, and deadlines. CI runs the suite with the race detector and real Redis, then builds the Docker image.

## Scope

This is a runnable gateway project, not an Internet-facing security boundary. The supplied setup uses plaintext gRPC within the local network and has no authentication layer. Terminate TLS and authenticate clients at a trusted ingress before exposing it. Behind an ingress, all clients may share its IP bucket unless you implement a trusted client-identity policy.

`/healthz` is a process liveness check, not a backend readiness probe. `/metrics` exposes JSON counters and should remain internal. Streaming RPCs are subject to the configured timeout; long-lived streams need a larger value. There is no service discovery, hot reload, automatic retry, or HTTP-to-gRPC transcoding.

The main implementation references are Go's [ReverseProxy](https://pkg.go.dev/net/http/httputil#ReverseProxy), the [gRPC Go API](https://pkg.go.dev/google.golang.org/grpc), and the [Redis Go client](https://github.com/redis/go-redis).
