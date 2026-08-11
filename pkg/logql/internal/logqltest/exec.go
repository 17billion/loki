package logqltest

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
)

// executionStack runs eval commands through one query path and reports how its results must be
// asserted.
type executionStack interface {
	// name identifies the stack in subtest output.
	name() string
	// setStreams (re)builds the stack's store with the provided log streams.
	setStreams(streams []logproto.Stream)
	// eval runs cmd and returns the query result.
	eval(cmd evalCmd) (logqlmodel.Result, error)
	// isQueryShardingSupported reports whether this stack runs queries with sharding enabled.
	isQueryShardingSupported() bool
	// isEvalSupported reports whether this stack can run the given cmd and exp.
	isEvalSupported(cmd evalCmd, exp expectations) bool
}

// isQueryShardingSupported reports whether a query is expected to fan out into >= 2 shards.
func isQueryShardingSupported(query string) bool {
	expr, err := syntax.ParseSampleExpr(query)
	if err != nil {
		return false
	}

	supported := true
	expr.Walk(func(e syntax.Expr) bool {
		switch ex := e.(type) {
		case *syntax.VectorExpr:
			// vector() is never sharded.
			supported = false
		case *syntax.RangeAggregationExpr:
			if !ex.Shardable(true) {
				supported = false
			}
		}
		return true
	})
	return supported
}

// newScriptStore builds a chunk store from streams and registers its close.
func newScriptStore(t *testing.T, streams []logproto.Stream) *testingChunkStore {
	store := newTestingChunkStore(t)

	// The close runs before the store's temp dir is removed: newTestingChunkStore
	// registers the temp-dir cleanup first, so this later-registered cleanup runs
	// first (t.Cleanup is LIFO).
	t.Cleanup(store.close)

	store.write(t, streams)
	store.flush(t)
	return store
}
