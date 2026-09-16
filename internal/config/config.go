package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
)

type Route struct {
	Prefix   string `json:"prefix"`
	Upstream string `json:"upstream"`
	Cache    bool   `json:"cache"`
}

type Config struct {
	HTTPAddr        string            `json:"http_addr"`
	GRPCAddr        string            `json:"grpc_addr"`
	RedisAddr       string            `json:"redis_addr"`
	Rate            float64           `json:"rate_per_second"`
	Burst           int               `json:"burst"`
	MaxClients      int               `json:"max_clients"`
	MaxConcurrent   int               `json:"max_concurrent"`
	TimeoutSeconds  int               `json:"timeout_seconds"`
	CacheTTLSeconds int               `json:"cache_ttl_seconds"`
	HTTPRoutes      []Route           `json:"http_routes"`
	GRPCRoutes      map[string]string `json:"grpc_routes"`
}

func Load(path string) (Config, error) {
	c := Config{HTTPAddr: ":8080", GRPCAddr: ":9090", RedisAddr: "localhost:6379", Rate: 1000, Burst: 2000, MaxClients: 10000, MaxConcurrent: 1024, TimeoutSeconds: 10, CacheTTLSeconds: 30}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, fmt.Errorf("config must contain one JSON object")
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		c.RedisAddr = addr
	}
	if c.HTTPAddr == "" || c.GRPCAddr == "" || c.Rate <= 0 || c.Burst < 1 || c.MaxClients < 1 || c.MaxConcurrent < 1 || c.TimeoutSeconds < 1 || c.CacheTTLSeconds < 1 {
		return c, fmt.Errorf("addresses and positive limits/timeouts are required")
	}
	seen := make(map[string]bool)
	for _, r := range c.HTTPRoutes {
		if !strings.HasPrefix(r.Prefix, "/") || strings.ContainsAny(r.Prefix, "?#") || seen[r.Prefix] {
			return c, fmt.Errorf("invalid or duplicate HTTP prefix %q", r.Prefix)
		}
		seen[r.Prefix] = true
		u, err := url.Parse(r.Upstream)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, fmt.Errorf("invalid upstream %q", r.Upstream)
		}
	}
	for service, target := range c.GRPCRoutes {
		if service == "" || strings.Contains(service, "/") || target == "" {
			return c, fmt.Errorf("invalid gRPC route %q", service)
		}
	}
	return c, nil
}
