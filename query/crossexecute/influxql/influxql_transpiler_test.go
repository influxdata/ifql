package influxql

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/plan"

	"bytes"
	"strings"
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
							Label: "_measurement",
							Type: execute.TString,
							Kind: execute.TagColKind,
							Common: true,
						},
						execute.ColMeta{
							Label: "_field",
							Type: execute.TString,
							Kind: execute.TagColKind,
							Common: true,
						},
						execute.ColMeta{
							Label: execute.DefaultValueColLabel,
							Type:  execute.TFloat,
							Kind:  execute.ValueColKind,
						},
					},
					Data: [][]interface{}{
						{execute.Time(5), "cpu", "max", 98.9},
					},
				}},
			},
			output: `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max"],"values":[["1970-01-01T00:00:00Z",98.9]]}]}]}`,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			results := map[string]execute.Result{}

			for k,v := range tc.blocks {
				results[k] = &testResult{v}
			}

			var resp bytes.Buffer
			var influxQLWriter Writer
			err := influxQLWriter.WriteTo(&resp, results)

			if err != nil {
				t.Error("error writing to buffer: ", err)
			}
			got := strings.TrimSpace(resp.String())
			if !cmp.Equal(got, tc.output) {
				t.Error("unexpected results -want/+got", cmp.Diff(tc.output, got))
			}
		})
	}
}

type testResult struct {
	blocks []*executetest.Block
}

func (r *testResult) Blocks() execute.BlockIterator {
	return &storageBlockIterator{
		storageReader{r.blocks},
	}
}

type storageReader struct {
	blocks []*executetest.Block
}

func (s storageReader) Close() {}


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
