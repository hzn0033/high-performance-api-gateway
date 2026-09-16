package ratelimit

import (
	"math"
	"sync"
	"time"
)

type bucket struct {
	tokens  float64
	updated time.Time
}

// Limiter keeps a token bucket per client. Limits are local to this process.
type Limiter struct {
	mu         sync.Mutex
	buckets    map[string]bucket
	rate       float64
	burst      float64
	maxClients int
	nextSweep  time.Time
}

func New(rate float64, burst, maxClients int) *Limiter {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) || burst < 1 || maxClients < 1 {
		panic("invalid token bucket settings")
	}
	return &Limiter{buckets: make(map[string]bucket), rate: rate, burst: float64(burst), maxClients: maxClients}
}

func (l *Limiter) Allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !now.Before(l.nextSweep) {
		// A fully refilled bucket can be removed without granting extra tokens.
		for k, b := range l.buckets {
			if b.tokens+now.Sub(b.updated).Seconds()*l.rate >= l.burst {
				delete(l.buckets, k)
			}
		}
		l.nextSweep = now.Add(time.Minute)
	}
	b, exists := l.buckets[key]
	if !exists {
		if len(l.buckets) >= l.maxClients {
			return false, time.Minute
		}
		b = bucket{tokens: l.burst, updated: now}
	}
	elapsed := math.Max(0, now.Sub(b.updated).Seconds())
	b.tokens = math.Min(l.burst, b.tokens+elapsed*l.rate)
	b.updated = now
	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	l.buckets[key] = b
	if allowed {
		return true, 0
	}
	return false, time.Duration(math.Ceil((1 - b.tokens) / l.rate * float64(time.Second)))
}
