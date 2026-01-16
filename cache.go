package main

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ChunkCacheEntry wraps cached chunk data with metadata for LRU eviction.
// It tracks the size and last access time without holding a reference to the data bytes
// (those are stored separately in the data map for lock-free reads).
type ChunkCacheEntry struct {
	Size           int64     // Size of the cached chunk in bytes
	LastAccessTime time.Time // When this chunk was last accessed
}

// LRUChunkCache implements bounded LRU eviction for read-ahead chunks.
// It maintains a split design:
// - data: sync.Map for lock-free reads of cached byte slices
// - metadata: sync.Map for tracking access times and sizes
// - evictionMutex: protects the eviction algorithm to avoid thundering herd
// - maxBytes: maximum total bytes allowed in cache
//
// This design minimizes lock contention on the hot read path while ensuring
// thread-safe eviction when needed. Cache operations are non-blocking; the
// eviction goroutine runs periodically to clean up old entries.
type LRUChunkCache struct {
	data          sync.Map // cacheKey -> []byte
	metadata      sync.Map // cacheKey -> *ChunkCacheEntry
	evictionMutex sync.Mutex
	currentBytes  int64 // Approximate current cache size (may be slightly stale)
	maxBytes      int64 // Maximum cache size in bytes
	logger        func(string, ...interface{})
	done          chan struct{}
	wg            sync.WaitGroup
}

// NewLRUChunkCache creates a new LRU cache with the specified max size.
// maxCacheBytes is the target maximum memory for cached chunks; actual memory
// may temporarily exceed this until the eviction goroutine runs.
// The eviction goroutine runs periodically to maintain the size limit.
func NewLRUChunkCache(maxCacheBytes int64, logger func(string, ...interface{})) *LRUChunkCache {
	cache := &LRUChunkCache{
		maxBytes: maxCacheBytes,
		logger:   logger,
		done:     make(chan struct{}),
	}

	// Start the background eviction goroutine
	cache.wg.Add(1)
	go cache.evictionWorker()

	return cache
}

// Get retrieves a chunk from the cache and updates its access time.
// Returns the cached data and true if found, nil and false if not found or evicted.
// This operation is non-blocking and lock-free for the common case.
func (c *LRUChunkCache) Get(cacheKey string) ([]byte, bool) {
	val, ok := c.data.Load(cacheKey)
	if !ok {
		return nil, false
	}

	// Update access time in metadata (non-blocking, may race but acceptable)
	if meta, ok := c.metadata.Load(cacheKey); ok {
		entry := meta.(*ChunkCacheEntry)
		entry.LastAccessTime = time.Now()
	}

	data := val.([]byte)
	return data, true
}

// Put stores a chunk in the cache with its size and current time.
// This operation is non-blocking. If the total cache size exceeds the limit,
// the eviction goroutine will clean up old entries in the background.
func (c *LRUChunkCache) Put(cacheKey string, data []byte) {
	dataSize := int64(len(data))

	c.data.Store(cacheKey, data)
	c.metadata.Store(cacheKey, &ChunkCacheEntry{
		Size:           dataSize,
		LastAccessTime: time.Now(),
	})

	// Atomically increase the size estimate
	newSize := atomic.AddInt64(&c.currentBytes, dataSize)

	// If we've significantly exceeded the limit, trigger eviction immediately
	// Use a threshold to avoid blocking on every insert
	if newSize > c.maxBytes*110/100 { // 10% over limit triggers immediate eviction
		c.tryEvict()
	}
}

// Delete removes a chunk from the cache entirely.
// This is used for cache invalidation when files are modified.
func (c *LRUChunkCache) Delete(cacheKey string) {
	if meta, ok := c.metadata.Load(cacheKey); ok {
		entry := meta.(*ChunkCacheEntry)
		atomic.AddInt64(&c.currentBytes, -entry.Size)
	}
	c.data.Delete(cacheKey)
	c.metadata.Delete(cacheKey)
}

// DeletePrefix removes all chunks matching a path prefix.
// This is used to clear all cached chunks for a file when it's modified or deleted.
func (c *LRUChunkCache) DeletePrefix(prefix string) {
	var toDelete []string

	c.data.Range(func(key, value interface{}) bool {
		if k, ok := key.(string); ok {
			if strings.HasPrefix(k, prefix) {
				toDelete = append(toDelete, k)
			}
		}
		return true
	})

	for _, key := range toDelete {
		c.Delete(key)
	}
}

// tryEvict performs an immediate eviction pass. It acquires the evictionMutex
// to prevent concurrent evictions and then removes the least-recently-used chunks
// until the cache is below the target size.
func (c *LRUChunkCache) tryEvict() {
	// Non-blocking lock acquire to avoid blocking the read path
	if !c.evictionMutex.TryLock() {
		// Another goroutine is already evicting, skip this pass
		return
	}
	defer c.evictionMutex.Unlock()

	// Collect all entries with their access times for sorting
	type entry struct {
		key      string
		accessed time.Time
		size     int64
	}
	var entries []entry

	c.metadata.Range(func(key, value interface{}) bool {
		if meta, ok := value.(*ChunkCacheEntry); ok {
			entries = append(entries, entry{
				key:      key.(string),
				accessed: meta.LastAccessTime,
				size:     meta.Size,
			})
		}
		return true
	})

	// Sort by access time (oldest first)
	slices.SortFunc(entries, func(a, b entry) int {
		if a.accessed.Before(b.accessed) {
			return -1
		} else if a.accessed.After(b.accessed) {
			return 1
		}
		return 0
	})

	// Evict oldest entries until we're below the target
	bytesFreed := int64(0)
	target := c.maxBytes * 90 / 100 // Target 90% of max to reduce eviction thrashing
	currentSize := atomic.LoadInt64(&c.currentBytes)

	for _, e := range entries {
		if currentSize-bytesFreed < target {
			break
		}

		// Double-check the entry still exists before deleting
		if _, ok := c.data.Load(e.key); ok {
			c.data.Delete(e.key)
			c.metadata.Delete(e.key)
			bytesFreed += e.size

			c.logger("CACHE EVICT: key=%s size=%d", e.key, e.size)
		}
	}

	// Update the current size estimate
	atomic.AddInt64(&c.currentBytes, -bytesFreed)
}

// evictionWorker runs periodically to check cache size and evict if needed.
// This background goroutine prevents the cache from growing unbounded without
// impacting the latency of read operations.
func (c *LRUChunkCache) evictionWorker() {
	defer c.wg.Done()

	ticker := time.NewTicker(CacheEvictionTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Periodic check; only evict if significantly over limit
			if atomic.LoadInt64(&c.currentBytes) > c.maxBytes {
				c.tryEvict()
			}
		case <-c.done:
			return
		}
	}
}

// Shutdown gracefully shuts down the eviction worker and waits for it to finish.
func (c *LRUChunkCache) Shutdown() {
	close(c.done)
	c.wg.Wait()
}
