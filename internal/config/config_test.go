package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInvalidConfigs(t *testing.T) {
	for _, body := range []string{
		`{"rate_per_second":0}`,
		`{"unknown":true}`,
		`{} {}`,
		`{"http_routes":[{"prefix":"api","upstream":"http://localhost"}]}`,
		`{"http_routes":[{"prefix":"/api","upstream":"file:///tmp"}]}`,
		`{"http_routes":[{"prefix":"/api","upstream":"http://localhost"},{"prefix":"/api","upstream":"http://localhost"}]}`,
		`{"grpc_routes":{"/bad":"localhost:9090"}}`,
	} {
		p := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(p, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(p); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestRedisEnvironmentOverride(t *testing.T) {
	t.Setenv("REDIS_ADDR", "redis.internal:6379")
	p := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(p, []byte(`{}`), 0600)
	c, err := Load(p)
	if err != nil || c.RedisAddr != "redis.internal:6379" {
		t.Fatalf("config=%+v err=%v", c, err)
	}
}
