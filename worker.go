package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// PrefetchTask represents a single S3 range read request to be processed by the worker pool.
// Path is the internal file path, ChunkIndex is the prefetch chunk number.
type PrefetchTask struct {
	Path       string
	ChunkIndex int64
}

// PrefetchWorkerPool manages bounded concurrent prefetch requests with deduplication.
// It spawns maxWorkers goroutines that consume tasks from a queue, preventing unbounded
// goroutine creation while maintaining the prefetchActive deduplication mechanism.
type PrefetchWorkerPool struct {
	taskQueue      chan PrefetchTask
	maxWorkers     int
	activeSet      sync.Map       // Tracks which chunks are being prefetched (cacheKey -> bool)
	done           chan struct{}  // Signal to shut down workers
	wg             sync.WaitGroup // Waits for workers to finish before close
	s3Client       *s3.Client     // S3 client for fetching chunks
	readAheadCache *LRUChunkCache // LRU cache for caching chunks
	readAheadSize  int64          // Size of each chunk
	logger         func(string, ...interface{})
}

// NewPrefetchWorkerPool creates and starts a new bounded worker pool for prefetch requests.
// maxWorkers controls the maximum number of concurrent S3 reads, and queueSize limits
// pending tasks to prevent unbounded memory growth.
func NewPrefetchWorkerPool(
	maxWorkers int,
	queueSize int,
	s3Client *s3.Client,
	readAheadCache *LRUChunkCache,
	readAheadSize int64,
	logger func(string, ...interface{}),
) *PrefetchWorkerPool {
	pool := &PrefetchWorkerPool{
		taskQueue:      make(chan PrefetchTask, queueSize),
		maxWorkers:     maxWorkers,
		done:           make(chan struct{}),
		s3Client:       s3Client,
		readAheadCache: readAheadCache,
		readAheadSize:  readAheadSize,
		logger:         logger,
	}

	// Start maxWorkers goroutines, each consuming from the shared task queue.
	for i := 0; i < maxWorkers; i++ {
		pool.wg.Add(1)
		go pool.worker(i)
	}

	return pool
}

// worker is a long-lived goroutine that processes prefetch tasks from the queue.
// It handles deduplication via the activeSet and ensures only one goroutine
// processes each chunk concurrently.
func (p *PrefetchWorkerPool) worker(id int) {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				// Channel closed, worker should exit
				return
			}
			p.processPrefetchTask(task)
		case <-p.done:
			// Shutdown signal received, worker should exit
			return
		}
	}
}

// processPrefetchTask fetches a single chunk from S3 and caches it.
// It uses activeSet for deduplication to avoid fetching the same chunk twice concurrently.
func (p *PrefetchWorkerPool) processPrefetchTask(task PrefetchTask) {
	cacheKey := fmt.Sprintf("%s\x00%d", task.Path, task.ChunkIndex)

	// Skip if already cached
	if _, exists := p.readAheadCache.Get(cacheKey); exists {
		return
	}

	// Skip if already being prefetched by another worker (deduplication)
	if _, active := p.activeSet.LoadOrStore(cacheKey, true); active {
		return
	}
	defer p.activeSet.Delete(cacheKey)

	data := make([]byte, p.readAheadSize)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	chunkOffset := task.ChunkIndex * p.readAheadSize
	byteRange := fmt.Sprintf("bytes=%d-%d", chunkOffset, chunkOffset+int64(len(data))-1)

	output, err := p.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("io"),
		Key:    aws.String(task.Path),
		Range:  aws.String(byteRange),
	})

	if err != nil {
		return
	}

	n, err := io.ReadFull(output.Body, data)
	output.Body.Close()

	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return
	}

	data = data[:n]
	p.readAheadCache.Put(cacheKey, data)
	// p.logger("PREFETCH: chunk=%d bytes=%d", task.ChunkIndex, n)
}

// SubmitPrefetch enqueues a prefetch task if not already cached or being fetched.
// If the queue is full, the task is silently dropped (non-blocking). This prevents
// unbounded queue growth under sustained high load.
func (p *PrefetchWorkerPool) SubmitPrefetch(path string, chunkIndex int64) {
	cacheKey := fmt.Sprintf("%s\x00%d", path, chunkIndex)

	// Quick check: skip if already cached
	if _, exists := p.readAheadCache.Get(cacheKey); exists {
		return
	}

	// Try to enqueue without blocking to avoid hot path latency
	select {
	case p.taskQueue <- PrefetchTask{Path: path, ChunkIndex: chunkIndex}:
		// Task enqueued successfully
	default:
		// Queue is full, drop task to prevent blocking the read path
	}
}

// Shutdown gracefully shuts down the worker pool. It closes the task queue,
// signals workers to exit, and waits for all workers to finish.
// This should be called before unmounting the filesystem.
func (p *PrefetchWorkerPool) Shutdown() {
	close(p.done)
	close(p.taskQueue)
	p.wg.Wait()
}
