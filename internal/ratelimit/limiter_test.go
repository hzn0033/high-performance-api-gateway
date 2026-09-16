package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRefillAndClientIsolation(t *testing.T) {
	l := New(2, 2, 10)
	now := time.Now()
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("a", now); !ok {
			t.Fatal("burst denied")
		}
	}
	if ok, retry := l.Allow("a", now); ok || retry != 500*time.Millisecond {
		t.Fatalf("allow=%v retry=%v", ok, retry)
	}
	if ok, _ := l.Allow("b", now); !ok {
		t.Fatal("clients share tokens")
	}
	if ok, _ := l.Allow("a", now.Add(500*time.Millisecond)); !ok {
		t.Fatal("token did not refill")
	}
	if ok, _ := l.Allow("a", now.Add(500*time.Millisecond)); ok {
		t.Fatal("extra token")
	}
}

func TestConcurrentBurst(t *testing.T) {
	l := New(1, 40, 100)
	now := time.Now()
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := l.Allow("client", now); ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 40 {
		t.Fatalf("allowed %d requests, want 40", allowed.Load())
	}
}

func TestClientBoundAndCleanup(t *testing.T) {
	l := New(1, 1, 1)
	now := time.Now()
	l.Allow("a", now)
	if ok, _ := l.Allow("b", now); ok {
		t.Fatal("client limit ignored")
	}
	if ok, _ := l.Allow("b", now.Add(time.Minute)); !ok {
		t.Fatal("idle bucket was not reclaimed")
	}
}

func BenchmarkLimiter(b *testing.B) {
	l := New(1e12, 1000000, 100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow("client", time.Now())
		}
	})
}
