package monitor

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	publicProxyCacheEntries = 64
	// publicProxyCacheBytes bounds the whole cache. Sixteen full-size entries
	// fit, which keeps worst-case memory predictable (16 MiB).
	publicProxyCacheBytes = 16 << 20
	// publicProxyCacheEntrySize must admit a full /proxies page of
	// publicProxyMaxLimit entries (roughly 200-400 bytes of JSON each), or the
	// most expensive responses would never be cached.
	publicProxyCacheEntrySize = 1 << 20
	publicProxyCacheTTL       = 30 * time.Second
	// publicProxyLoadTimeout bounds a shared load. The load is detached from
	// the cancellation of the request that started it so one disconnecting
	// client cannot fail every waiter coalesced onto the same flight.
	publicProxyLoadTimeout = readyzTimeout
	// publicProxyLoadSlotWait is how long a cache miss waits for a free load
	// slot before it is shed. It only smooths short bursts; the wait is not
	// tied to the request context for the same reason loads are detached.
	publicProxyLoadSlotWait = 250 * time.Millisecond
	// publicProxyMaxLoadSlots caps concurrent store loads however large the
	// Redis API pool is configured.
	publicProxyMaxLoadSlots = 6
)

// errPublicProxyBusy reports that too many distinct cache misses are already
// querying the store, so this one was shed instead of queued.
var errPublicProxyBusy = errors.New("public proxy query capacity exhausted")

// publicProxyLoadSlots sizes the cache-miss load limit from the HTTP read
// pool: at most half of it, so /readyz, /stats, /report, and the internal API
// always find a free connection no matter how many distinct filters arrive.
func publicProxyLoadSlots(apiPoolSize int) int {
	slots := apiPoolSize / 2
	if slots < 1 {
		slots = 1
	}
	if slots > publicProxyMaxLoadSlots {
		slots = publicProxyMaxLoadSlots
	}
	return slots
}

// publicProxyResponseCache is a small, in-process LRU cache of successful
// public proxy responses. Its mutex protects every field, including the LRU.
type publicProxyResponseCache struct {
	mu            sync.Mutex
	entries       map[[sha256.Size]byte]*list.Element
	inFlight      map[[sha256.Size]byte]*publicProxyCacheFlight
	order         *list.List
	maxEntries    int
	maxInFlight   int
	maxBytes      int
	maxEntryBytes int
	ttl           time.Duration
	now           func() time.Time
	bytes         int
	// loadSlots, when non-nil, bounds concurrent load calls. Each distinct
	// filter is a separate cache key, so without it a client rotating a
	// filter value turns every request into a store scan.
	loadSlots chan struct{}
}

// limitLoads bounds concurrent loads to n (n <= 0 removes the bound). Call it
// before the cache serves requests.
func (c *publicProxyResponseCache) limitLoads(n int) {
	if n <= 0 {
		c.loadSlots = nil
		return
	}
	c.loadSlots = make(chan struct{}, n)
}

// acquireLoadSlot reserves a load slot, waiting briefly for one, and returns
// its release func. A full cache sheds the load with errPublicProxyBusy.
func (c *publicProxyResponseCache) acquireLoadSlot() (func(), error) {
	if c.loadSlots == nil {
		return func() {}, nil
	}
	release := func() { <-c.loadSlots }
	select {
	case c.loadSlots <- struct{}{}:
		return release, nil
	default:
	}
	timer := time.NewTimer(publicProxyLoadSlotWait)
	defer timer.Stop()
	select {
	case c.loadSlots <- struct{}{}:
		return release, nil
	case <-timer.C:
		return nil, errPublicProxyBusy
	}
}

type publicProxyCacheFlight struct {
	done chan struct{}
	body []byte
	err  error
}

type publicProxyCacheEntry struct {
	key       [sha256.Size]byte
	body      []byte
	expiresAt time.Time
}

func newPublicProxyResponseCache(maxEntries, maxBytes, maxEntryBytes int, ttl time.Duration, now func() time.Time) *publicProxyResponseCache {
	if now == nil {
		now = time.Now
	}
	return &publicProxyResponseCache{
		entries:       make(map[[sha256.Size]byte]*list.Element, maxEntries),
		inFlight:      make(map[[sha256.Size]byte]*publicProxyCacheFlight),
		order:         list.New(),
		maxEntries:    maxEntries,
		maxInFlight:   maxEntries,
		maxBytes:      maxBytes,
		maxEntryBytes: maxEntryBytes,
		ttl:           ttl,
		now:           now,
	}
}

func publicProxyCacheKey(filter store.ProxyFilter, offset int) [sha256.Size]byte {
	// Hashing a length-delimited representation keeps keys fixed-size even for
	// unusually large parsed filter values, while retaining every filter field.
	hash := sha256.New()
	writeString := func(value string) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	writeInt64 := func(value int64) {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(value))
		_, _ = hash.Write(encoded[:])
	}
	writeString(filter.Country)
	writeString(filter.ExitCountry)
	writeString(filter.ASN)
	writeString(filter.Anonymity)
	if filter.GeoMismatch {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	writeInt64(filter.MaxLatencyMs)
	writeInt64(filter.MinRatioPct)
	writeInt64(filter.MaxAgeMs)
	if filter.HTTPS {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	if filter.ExcludeTampered {
		_, _ = hash.Write([]byte{1})
	} else {
		_, _ = hash.Write([]byte{0})
	}
	writeInt64(filter.ClassificationMaxAgeMs)
	if filter.Limit > 0 {
		writeInt64(int64(filter.Limit))
	} else {
		writeInt64(0)
	}
	if offset > 0 {
		writeInt64(int64(offset))
	} else {
		writeInt64(0)
	}

	var key [sha256.Size]byte
	copy(key[:], hash.Sum(nil))
	return key
}

func (c *publicProxyResponseCache) get(key [sha256.Size]byte) ([]byte, time.Duration, bool) {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getLocked(key, now)
}

func (c *publicProxyResponseCache) getLocked(key [sha256.Size]byte, now time.Time) ([]byte, time.Duration, bool) {
	element, ok := c.entries[key]
	if !ok {
		return nil, 0, false
	}
	entry := element.Value.(*publicProxyCacheEntry)
	if !now.Before(entry.expiresAt) {
		c.remove(element)
		return nil, 0, false
	}
	c.order.MoveToFront(element)
	return entry.body, entry.expiresAt.Sub(now), true
}

// getOrLoad coalesces concurrent cache misses for a key. Failed loads are
// shared with current waiters but are never retained in the response cache.
// Distinct misses run at most loadSlots at a time; the rest fail fast with
// errPublicProxyBusy, including when the in-flight table is full.
// ctx only bounds how long this caller waits; load receives a context that
// keeps ctx's values but not its cancellation, bounded by
// publicProxyLoadTimeout, because its result is shared with other callers.
func (c *publicProxyResponseCache) getOrLoad(ctx context.Context, key [sha256.Size]byte, load func(context.Context) ([]byte, error)) ([]byte, time.Duration, bool, error) {
	runLoad := func() ([]byte, error) {
		release, err := c.acquireLoadSlot()
		if err != nil {
			return nil, err
		}
		defer release()
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publicProxyLoadTimeout)
		defer cancel()
		return load(loadCtx)
	}
	now := c.now()
	c.mu.Lock()
	if body, remaining, ok := c.getLocked(key, now); ok {
		c.mu.Unlock()
		return body, remaining, true, nil
	}
	if flight := c.inFlight[key]; flight != nil {
		c.mu.Unlock()
		select {
		case <-flight.done:
			if flight.err != nil {
				return nil, 0, false, flight.err
			}
			if body, remaining, ok := c.get(key); ok {
				return body, remaining, true, nil
			}
			return flight.body, 0, false, nil
		case <-ctx.Done():
			return nil, 0, false, ctx.Err()
		}
	}
	if len(c.inFlight) >= c.maxInFlight {
		c.mu.Unlock()
		body, err := runLoad()
		if err == nil {
			c.put(key, body)
		}
		return body, 0, false, err
	}
	flight := &publicProxyCacheFlight{done: make(chan struct{})}
	c.inFlight[key] = flight
	c.mu.Unlock()

	body, err := runLoad()
	if err == nil {
		c.put(key, body)
	}
	c.mu.Lock()
	flight.body = body
	flight.err = err
	delete(c.inFlight, key)
	close(flight.done)
	c.mu.Unlock()
	return body, 0, false, err
}

func (c *publicProxyResponseCache) put(key [sha256.Size]byte, body []byte) {
	if len(body) > c.maxEntryBytes || len(body) > c.maxBytes || c.maxEntries <= 0 || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		c.remove(element)
	}
	for c.order.Len() >= c.maxEntries || c.bytes+len(body) > c.maxBytes {
		c.remove(c.order.Back())
	}
	entry := &publicProxyCacheEntry{key: key, body: body, expiresAt: c.now().Add(c.ttl)}
	c.entries[key] = c.order.PushFront(entry)
	c.bytes += len(body)
}

func (c *publicProxyResponseCache) remove(element *list.Element) {
	entry := element.Value.(*publicProxyCacheEntry)
	delete(c.entries, entry.key)
	c.order.Remove(element)
	c.bytes -= len(entry.body)
}
