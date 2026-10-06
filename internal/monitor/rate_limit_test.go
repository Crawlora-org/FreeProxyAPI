package monitor

import (
	"fmt"
	"testing"
	"time"
)

func TestIPRateLimiterSeparatesClientsAndWindows(t *testing.T) {
	limiter := newIPRateLimiter(2, time.Minute)
	now := time.Unix(100, 0)

	for i := 1; i <= 2; i++ {
		if !limiter.allow("203.0.113.10", now) {
			t.Fatalf("request %d from a client should be allowed", i)
		}
	}
	if limiter.allow("203.0.113.10", now) {
		t.Fatal("third request from a client should be rate limited")
	}
	if !limiter.allow("203.0.113.11", now) {
		t.Fatal("a separate client should have its own window")
	}
	if !limiter.allow("203.0.113.10", now.Add(time.Minute)) {
		t.Fatal("client should be allowed in a new window")
	}
}

func TestIPRateLimiterBoundsClientsAndRecoversIncrementally(t *testing.T) {
	limiter := newIPRateLimiter(1, time.Minute)
	limiter.maxClients = 100
	now := time.Unix(100, 0)
	for i := 0; i < limiter.maxClients; i++ {
		if !limiter.allow(fmt.Sprintf("192.0.2.%d", i), now) {
			t.Fatalf("client %d unexpectedly denied", i)
		}
	}
	// A full table must not lock out new clients: the oldest window is
	// evicted, and recent clients keep their counts.
	if !limiter.allow("198.51.100.1", now) || len(limiter.clients) != limiter.maxClients {
		t.Fatalf("new client denied at capacity: size=%d", len(limiter.clients))
	}
	if _, ok := limiter.clients["192.0.2.0"]; ok {
		t.Fatal("oldest client was not evicted at capacity")
	}
	if limiter.allow("192.0.2.99", now) {
		t.Fatal("recent client lost its window when the table was full")
	}
	if !limiter.allow("198.51.100.2", now.Add(time.Minute)) {
		t.Fatal("limiter did not admit a client after old windows expired")
	}
	if len(limiter.clients) != 37 {
		t.Fatalf("cleanup was not bounded to 64 entries: size=%d", len(limiter.clients))
	}
}

func TestIPRateLimiterGroupsIPv6By64(t *testing.T) {
	limiter := newIPRateLimiter(1, time.Minute)
	now := time.Unix(100, 0)
	if !limiter.allow("2001:db8:1:2::1", now) {
		t.Fatal("first IPv6 request should be allowed")
	}
	if limiter.allow("2001:db8:1:2:ffff:ffff:ffff:ffff", now) {
		t.Fatal("another address in the same /64 should share the window")
	}
	if !limiter.allow("2001:db8:1:3::1", now) {
		t.Fatal("a different /64 should have its own window")
	}
	if !limiter.allow("203.0.113.10", now) {
		t.Fatal("IPv4 clients should be keyed individually")
	}
}
