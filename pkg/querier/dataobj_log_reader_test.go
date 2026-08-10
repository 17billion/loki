package querier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/loki/pkg/push"

	"github.com/grafana/loki/v3/pkg/dataobj/sections/logs"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

// newTestLogReaderBatches builds a dataObjLogReader that yields the given batches then reports err,
// without opening a real data object. The batches are pre-buffered and the channel is closed, so no
// scan goroutine runs and Next walks the buffer before reporting EOF.
func newTestLogReaderBatches(batches [][]dataObjLogRecord, err error) *dataObjLogReader {
	ch := make(chan []dataObjLogRecord, len(batches))
	for _, b := range batches {
		ch <- b
	}
	close(ch)

	stopped := make(chan struct{})
	close(stopped)

	return &dataObjLogReader{
		cache:       newDataObjCache(nil, "test"),
		nextBatches: ch,
		stopped:     stopped,
		cancel:      func() {},
		err:         err,
	}
}

// TestDataObjLogReader_MultiObject drives the planner + log reader over two objects where one stream
// spans both, and asserts every matching line is yielded exactly once with the right fingerprint and
// timestamp. Order is not asserted (the reader is unordered). Run under -race to exercise the
// concurrent per-section fan-in.
func TestDataObjLogReader_MultiObject(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), dataObjTestTenant)

	a := labels.FromStrings("job", "t", "app", "a")
	b := labels.FromStrings("job", "t", "app", "b")
	c := labels.FromStrings("job", "t", "app", "c")

	at := func(sec int64) time.Time { return time.Unix(sec, 0) }

	bucket := objstore.NewInMemBucket()
	// Stream a spans both objects; b only in obj1; c only in obj2.
	ms := newTestDataObjMetastore(ctx, t, bucket, testSectionSize, [][]logproto.Stream{
		{
			{Labels: a.String(), Entries: []push.Entry{{Timestamp: at(1), Line: "a1"}, {Timestamp: at(3), Line: "a3"}}},
			{Labels: b.String(), Entries: []push.Entry{{Timestamp: at(2), Line: "b2"}}},
		},
		{
			{Labels: a.String(), Entries: []push.Entry{{Timestamp: at(5), Line: "a5"}}},
			{Labels: c.String(), Entries: []push.Entry{{Timestamp: at(6), Line: "c6"}, {Timestamp: at(7), Line: "c7"}}},
		},
	})

	cache := newDataObjCache(bucket, dataObjTestTenant)
	expr, err := syntax.ParseSampleExpr(`count_over_time({job="t"}[1h])`)
	require.NoError(t, err)
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "t")}
	tasks, err := newDataObjReadPlanner(ms, cache).Plan(ctx, at(0), at(100), matchers, nil, expr)
	require.NoError(t, err)

	// A small batch size forces multiple batches per section, exercising the batch boundary.
	reader := newDataObjLogReader(ctx, cache, tasks, defaultMaxConcurrency, 2)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	type key struct {
		fp uint64
		ts int64
	}
	got := map[key]int{}
	for reader.Next() {
		r := reader.At()
		got[key{r.fingerprint, r.timestamp}]++
	}
	require.NoError(t, reader.Err())

	fp := labels.StableHash
	want := map[key]int{
		{fp(a), at(1).UnixNano()}: 1,
		{fp(a), at(3).UnixNano()}: 1,
		{fp(a), at(5).UnixNano()}: 1,
		{fp(b), at(2).UnixNano()}: 1,
		{fp(c), at(6).UnixNano()}: 1,
		{fp(c), at(7).UnixNano()}: 1,
	}
	require.Equal(t, want, got, "every matching line must be yielded exactly once, with its fingerprint")
}

// TestDataObjLogReader_ConcurrentSections reads one object split into many logs sections with the
// default concurrency, so several section-read goroutines share and lazily open sections on the same
// cached openObject at once. Run under -race: without a lock on openObject the shared logsSec map races
// (a fatal concurrent map write in production). It also asserts every record is yielded exactly once.
func TestDataObjLogReader_ConcurrentSections(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), dataObjTestTenant)

	at := func(sec int64) time.Time { return time.Unix(sec, 0) }
	apps := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	objStreams := make([]logproto.Stream, 0, len(apps))
	for i, app := range apps {
		lbls := labels.FromStrings("job", "t", "app", app)
		objStreams = append(objStreams, logproto.Stream{
			Labels:  lbls.String(),
			Entries: []push.Entry{{Timestamp: at(int64(i + 1)), Line: app}},
		})
	}

	bucket := objstore.NewInMemBucket()
	// A section size of 1 byte forces one logs section per stream, so the single object splits into
	// several concurrently-read sections.
	ms := newTestDataObjMetastore(ctx, t, bucket, 1, [][]logproto.Stream{objStreams})

	cache := newDataObjCache(bucket, dataObjTestTenant)
	expr, err := syntax.ParseSampleExpr(`count_over_time({job="t"}[1h])`)
	require.NoError(t, err)
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "job", "t")}
	tasks, err := newDataObjReadPlanner(ms, cache).Plan(ctx, at(0), at(100), matchers, nil, expr)
	require.NoError(t, err)
	require.Greater(t, len(tasks), 1, "a tiny section size must split the object into several sections")
	for _, task := range tasks {
		require.Equal(t, tasks[0].object, task.object, "every task must read the same object")
	}

	reader := newDataObjLogReader(ctx, cache, tasks, defaultMaxConcurrency, defaultReadBatchSize)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	// A count_over_time projects stream_id + timestamp (not the message), so assert on the per-stream
	// timestamps rather than the line: each stream's distinct timestamp must appear exactly once.
	got := map[int64]int{}
	for reader.Next() {
		got[reader.At().timestamp]++
	}
	require.NoError(t, reader.Err())

	want := map[int64]int{}
	for i := range apps {
		want[at(int64(i+1)).UnixNano()] = 1
	}
	require.Equal(t, want, got, "every record must be yielded exactly once across the concurrent sections")
}

func TestDataObjLogReader_Batches(t *testing.T) {
	a := labels.FromStrings("app", "a")
	rec := func(ts int64) dataObjLogRecord { return testLogRecord(1, a, ts, "line") }

	tests := map[string]struct {
		batches [][]dataObjLogRecord
		want    []int64 // record timestamps, in emission order
	}{
		"no batches": {
			batches: nil,
			want:    nil,
		},
		"single batch": {
			batches: [][]dataObjLogRecord{{rec(1), rec(2)}},
			want:    []int64{1, 2},
		},
		"multiple batches walk in order": {
			batches: [][]dataObjLogRecord{{rec(1)}, {rec(2), rec(3)}},
			want:    []int64{1, 2, 3},
		},
		"empty batches are skipped": {
			batches: [][]dataObjLogRecord{{}, {rec(1)}, {}, {rec(2)}, {}},
			want:    []int64{1, 2},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reader := newTestLogReaderBatches(tc.batches, nil)
			var got []int64
			for reader.Next() {
				got = append(got, reader.At().timestamp)
			}
			require.Equal(t, tc.want, got)
			require.NoError(t, reader.Err())
		})
	}
}

func TestDataObjLogReader_Err(t *testing.T) {
	wantErr := errors.New("scan failed")
	batch := []dataObjLogRecord{testLogRecord(1, labels.FromStrings("app", "a"), 1, "line")}
	reader := newTestLogReaderBatches([][]dataObjLogRecord{batch}, wantErr)

	var n int
	for reader.Next() {
		n++
	}
	require.Zero(t, n, "a recorded error stops iteration without draining the queued batch")
	require.ErrorIs(t, reader.Err(), wantErr)
}

// TestDataObjLogReader_UnexpectedStreamID drives a real section scan whose task knows only one of the
// two streams it reads, and asserts the unexpected stream surfaces as an error through reader.Err()
// rather than being silently dropped.
func TestDataObjLogReader_UnexpectedStreamID(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), dataObjTestTenant)
	a := labels.FromStrings("app", "a")
	b := labels.FromStrings("app", "b")

	bucket := objstore.NewInMemBucket()
	ids := buildDataObject(ctx, t, bucket, "obj1", []logproto.Stream{
		{Labels: a.String(), Entries: []push.Entry{{Timestamp: time.Unix(1, 0), Line: "a1"}}},
		{Labels: b.String(), Entries: []push.Entry{{Timestamp: time.Unix(2, 0), Line: "b2"}}},
	})
	require.Len(t, ids, 2)

	// The task reads both streams (MatchStreams) but records the fingerprint of only the first.
	task := dataObjReadTask{
		object:           "obj1",
		section:          0,
		streamIDs:        []streamID{streamID(ids[0]), streamID(ids[1])},
		fingerprints:     map[streamID]uint64{streamID(ids[0]): 1},
		labels:           map[streamID]labels.Labels{streamID(ids[0]): a},
		projectedColumns: []logs.ColumnType{logs.ColumnTypeStreamID, logs.ColumnTypeTimestamp},
		start:            time.Unix(0, 0),
		end:              time.Unix(100, 0),
	}

	reader := newDataObjLogReader(ctx, newDataObjCache(bucket, dataObjTestTenant), []dataObjReadTask{task}, 1, defaultReadBatchSize)
	// Close surfaces the scan error, which this test expects and asserts via reader.Err() below.
	t.Cleanup(func() { _ = reader.Close() })

	for reader.Next() {
		_ = reader.At()
	}
	require.ErrorContains(t, reader.Err(), "unexpected stream ID")
}
