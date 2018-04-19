package influxql

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/plan"

	"bytes"
)
var epoch = time.Unix(0, 0)

func TestExecutor_Execute(t *testing.T) {
	testCases := []struct {
		name string
		query string
		blocks  map[string][]*executetest.Block
		output string
	}{
		{
			query: "",
			name: "one result one row",

			blocks: map[string][]*executetest.Block{
				plan.DefaultYieldName: []*executetest.Block{{
					Bnds: execute.Bounds{
						Start: 1,
						Stop:  5,
					},
					ColMeta: []execute.ColMeta{
						execute.TimeCol,
						execute.ColMeta{
							Label: execute.DefaultValueColLabel,
							Type:  execute.TFloat,
							Kind:  execute.ValueColKind,
						},
					},
					Data: [][]interface{}{
						{execute.Time(5), 15.0},
					},
				}},
			},
			output: ``,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			results := map[string]*testResult{}

			for k,v := range tc.blocks {
				results[k] = &testResult{v}
			}

			var resp bytes.Buffer
			var influxQLWriter Writer
			err := influxQLWriter.WriteTo(&resp, results)

			if !cmp.Equal(got, tc.exp) {
				t.Error("unexpected results -want/+got", cmp.Diff(tc.exp, got))
			}
		})
	}
}

type testResult struct {
	blocks []*executetest.Block
}

func (r testResult) Blocks() execute.BlockIterator {
	return &storageBlockIterator{
		storageReader{r.blocks},
	}
}

func (r testResult) abort(err error) {}

type storageReader struct {
	blocks []*executetest.Block
}

func (s storageReader) Close() {}
func (s storageReader) Read(context.Context, map[string]string, execute.ReadSpec, execute.Time, execute.Time) (execute.BlockIterator, error) {
	return &storageBlockIterator{
		s: s,
	}, nil
}

type storageBlockIterator struct {
	s storageReader
}

func (bi *storageBlockIterator) Do(f func(execute.Block) error) error {
	for _, b := range bi.s.blocks {
		if err := f(b); err != nil {
			return err
		}
	}
	return nil
}
