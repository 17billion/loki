package querier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
)

const (
	// defaultMaxConcurrency is how many logs sections the reader scans at once. Reads are
	// object-storage I/O bound, so a high fan-out hides latency.
	defaultMaxConcurrency = 16

	// defaultReadBatchSize is how many records each section read decodes and forwards as one batch.
	defaultReadBatchSize = 1024
)

// dataObjLogRecord is one decoded log line with the identity the sample layer needs.
type dataObjLogRecord struct {
	fingerprint  uint64
	streamLabels labels.Labels
	timestamp    int64
	line         []byte
	metadata     labels.Labels
}

// dataObjLogReader executes a read plan and yields decoded log lines in batches. Each section is
// scanned exactly once by one RowReader (all its shard-filtered streams matched together, projected
// columns only), and up to maxConcurrency sections are scanned concurrently to hide object-storage
// latency.
//
// Records are forwarded one batch at a time — one batch per section read — so the per-line hand-off
// cost stays negligible at millions of lines per second. The consumer (the sample iterator and the
// stream-first range-vector evaluator) is order-independent: the evaluator folds each sample into a
// per-(series, step) reduction regardless of arrival order, so batches from different sections may
// interleave freely. Within a batch the rows keep the section's stream-clustered order, so the
// evaluator's same-series fast path still hits. Memory is bounded by the batch channel plus the
// in-flight readers' page batches — independent of stream and sample counts.
type dataObjLogReader struct {
	cache *dataObjCache

	nextBatches chan []dataObjLogRecord

	// stopped is closed when the background scan goroutine has fully exited. Close waits on it so the
	// object cache is released only after every scan has stopped using it.
	stopped chan struct{}

	cancel context.CancelFunc

	errMu sync.Mutex
	err   error

	currBatch []dataObjLogRecord
	currPos   int
}

func newDataObjLogReader(ctx context.Context, cache *dataObjCache, tasks []dataObjReadTask, maxConcurrency, batchSize int) *dataObjLogReader {
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	r := &dataObjLogReader{
		cache:       cache,
		nextBatches: make(chan []dataObjLogRecord, maxConcurrency),
		stopped:     make(chan struct{}),
		cancel:      cancel,
	}

	go r.runTasks(ctx, tasks, maxConcurrency, batchSize)
	return r
}

func (r *dataObjLogReader) runTasks(ctx context.Context, tasks []dataObjReadTask, maxConcurrency, batchSize int) {
	defer close(r.stopped)
	defer close(r.nextBatches)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrency)
	for _, task := range tasks {
		g.Go(func() error {
			err := r.runTask(ctx, task, batchSize)
			if err != nil {
				// Record the error the moment a scan fails, so Next can stop without draining the
				// batches queued before the failure. Returning it cancels the group context, which
				// unblocks the other scans' pending sends.
				r.setErr(err)
			}
			return err
		})
	}
	_ = g.Wait() // Errors are recorded above; wait only so the channel closes after every scan stops.
}

func (r *dataObjLogReader) runTask(ctx context.Context, task dataObjReadTask, batchSize int) error {
	obj, err := r.cache.get(ctx, task.object)
	if err != nil {
		return err
	}
	section, err := obj.logsSection(ctx, task.section)
	if err != nil {
		return err
	}
	if section == nil {
		return fmt.Errorf("logs section %d not found in data object %q", task.section, task.object)
	}

	reader := logs.NewRowReader(section)
	defer reader.Close()
	if err := reader.SetColumns(task.projectedColumns, task.projectedMetadata); err != nil {
		return err
	}
	if err := reader.MatchStreams(slices.Values(streamIDsToInt64(task.streamIDs))); err != nil {
		return err
	}
	if err := reader.SetPredicates(task.rowPredicates()); err != nil {
		return err
	}
	if err := reader.Open(ctx); err != nil {
		return err
	}

	buf := make([]logs.Record, batchSize)
	for {
		n, err := reader.Read(ctx, buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		batch, batchErr := task.recordBatch(buf[:n])
		if batchErr != nil {
			return batchErr
		}
		if len(batch) > 0 {
			select {
			case r.nextBatches <- batch:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if n == 0 && errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func (r *dataObjLogReader) Next() bool {
	for {
		if r.currPos+1 < len(r.currBatch) {
			r.currPos++
			return true
		}
		// A failed scan is terminal, so stop rather than return batches queued before the failure.
		// Checked once per batch, not per record, to keep the per-sample path cheap.
		if r.Err() != nil {
			return false
		}
		batch, ok := <-r.nextBatches
		if !ok {
			return false
		}
		r.currBatch = batch
		r.currPos = 0
		if len(r.currBatch) > 0 {
			return true
		}
	}
}

func (r *dataObjLogReader) At() dataObjLogRecord { return r.currBatch[r.currPos] }

func (r *dataObjLogReader) Err() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	return r.err
}

func (r *dataObjLogReader) setErr(err error) {
	r.errMu.Lock()
	defer r.errMu.Unlock()
	if r.err == nil {
		r.err = err
	}
}

// Close stops the scan workers and releases the object cache. It blocks until the workers have exited.
func (r *dataObjLogReader) Close() error {
	r.cancel()
	<-r.stopped
	r.cache.Close()
	return r.Err()
}
