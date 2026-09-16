// loadtest compares the same request mix with cache bypassed and enabled.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"
)

type sample struct {
	duration time.Duration
	status   int
	hit      bool
}
type result struct {
	Mode              string      `json:"mode"`
	Requests          int         `json:"requests"`
	Successful        int         `json:"successful"`
	CacheHits         int         `json:"cache_hits"`
	UpstreamRequests  int64       `json:"upstream_requests"`
	Seconds           float64     `json:"seconds"`
	RequestsPerSecond float64     `json:"requests_per_second"`
	P50MS             float64     `json:"p50_ms"`
	P95MS             float64     `json:"p95_ms"`
	P99MS             float64     `json:"p99_ms"`
	Statuses          map[int]int `json:"statuses"`
}

func main() {
	base := flag.String("url", "http://localhost:8080", "gateway HTTP address")
	n := flag.Int("n", 1000, "requests per run")
	unique := flag.Int("unique", 600, "distinct items; remaining requests repeat them")
	workers := flag.Int("concurrency", 16, "concurrent clients")
	flag.Parse()
	if *n < 1 || *unique < 1 || *unique > *n || *workers < 1 {
		log.Fatal("require n >= unique >= 1 and concurrency >= 1")
	}
	u, err := url.Parse(*base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		log.Fatal("invalid URL")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = *workers
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}
	defer transport.CloseIdleConnections()
	stats := func() int64 {
		resp, err := client.Get(*base + "/metrics")
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]int64
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil || resp.StatusCode != 200 {
			log.Fatal("cannot read gateway metrics")
		}
		return m["http_upstream_requests"]
	}
	var results []result
	for _, bypass := range []bool{true, false} {
		mode := "cached"
		if bypass {
			mode = "bypass"
		}
		r := result{Mode: mode, Requests: *n, Statuses: make(map[int]int)}
		nonce := fmt.Sprintf("%d-%s", time.Now().UnixNano(), mode)
		before := stats()
		start := time.Now()
		var samples []sample
		// Finish first visits before repeats to make the cache-hit ratio reproducible.
		for _, bounds := range [][2]int{{0, *unique}, {*unique, *n}} {
			jobs := make(chan int)
			completed := make(chan sample, *n)
			var wg sync.WaitGroup
			for w := 0; w < *workers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range jobs {
						req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/items/%d?run=%s", *base, i%*unique, nonce), nil)
						if bypass {
							req.Header.Set("Cache-Control", "no-cache")
						}
						started := time.Now()
						resp, err := client.Do(req)
						s := sample{}
						if err == nil {
							_, err = io.Copy(io.Discard, resp.Body)
							resp.Body.Close()
							if err == nil {
								s.status = resp.StatusCode
								s.hit = resp.Header.Get("X-Cache") == "HIT"
							}
						}
						s.duration = time.Since(started)
						completed <- s
					}
				}()
			}
			for i := bounds[0]; i < bounds[1]; i++ {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			close(completed)
			for s := range completed {
				samples = append(samples, s)
			}
		}
		r.Seconds = time.Since(start).Seconds()
		r.UpstreamRequests = stats() - before
		durations := make([]time.Duration, 0, len(samples))
		for _, s := range samples {
			durations = append(durations, s.duration)
			r.Statuses[s.status]++
			if s.status >= 200 && s.status < 300 {
				r.Successful++
			}
			if s.hit {
				r.CacheHits++
			}
		}
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		percentile := func(p float64) float64 {
			return float64(durations[int(float64(len(durations)-1)*p)]) / float64(time.Millisecond)
		}
		r.P50MS = percentile(.50)
		r.P95MS = percentile(.95)
		r.P99MS = percentile(.99)
		r.RequestsPerSecond = float64(*n) / r.Seconds
		results = append(results, r)
	}
	reduction := 0.0
	if results[0].UpstreamRequests > 0 {
		reduction = 100 * (1 - float64(results[1].UpstreamRequests)/float64(results[0].UpstreamRequests))
	}
	valid := results[0].Successful == *n && results[1].Successful == *n
	output := map[string]any{"results": results, "downstream_reduction_percent": reduction, "all_requests_succeeded": valid, "unique_items": *unique, "concurrency": *workers, "note": "Synthetic two-phase workload. Latencies are end-to-end HTTP timings, not routing overhead. Run against an otherwise idle gateway."}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	encoder.Encode(output)
	if !valid {
		os.Exit(1)
	}
}
