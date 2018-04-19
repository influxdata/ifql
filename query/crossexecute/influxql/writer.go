package influxql

import (
	"encoding/json"
	"io"
	"log"
	"sort"
	"time"

	"github.com/influxdata/ifql/query/execute"
)

type Writer struct{}

type response struct {
	Results []Result `json:"results"`
	Err     string   `json:"err"`
}


func (Writer) WriteTo(w io.Writer, results map[string]execute.Result) error {
	// TODO: This code was copy pasted from the hackathon branch that enabled Chronograf to talk to IFQL.
	// It may need a refactor and it will tests to ensure that it accurately transforms the data to the correct format.
	resp := response{
		Results: make([]Result, len(results)),
	}

	// This process needs to be deterministic.
	// Process results in lexicographical order.
	order := make([]string, 0, len(results))
	for name := range results {
		order = append(order, name)
	}
	sort.Strings(order)

	for i, name := range order {
		r := results[name]
		blocks := r.Blocks()
		result := Result{StatementID: i}

		err := blocks.Do(func(b execute.Block) error {
			var r Row

			for k, v := range b.Tags() {
				if k == "_measurement" {
					r.Name = v
				} else if k != "_field" {
					r.Tags[k] = v
				}
			}

			for _, c := range b.Cols() {
				if !c.Common {
					r.Columns = append(r.Columns, c.Label)
				}

			}

			times := b.Times()
			times.DoTime(func(ts []execute.Time, rr execute.RowReader) {
				for i := range ts {
					var v []interface{}
					for j, c := range rr.Cols() {
						if c.Common {
							continue
						}
						switch c.Type {
						case execute.TFloat:
							v = append(v, rr.AtFloat(i, j))
						case execute.TInt:
							v = append(v, rr.AtInt(i, j))
						case execute.TString:
							v = append(v, rr.AtString(i, j))
						case execute.TUInt:
							v = append(v, rr.AtUInt(i, j))
						case execute.TBool:
							v = append(v, rr.AtBool(i, j))
						case execute.TTime:
							v = append(v, rr.AtTime(i, j)/execute.Time(time.Second))
						default:
							v = append(v, "unknown")
						}
					}

					r.Values = append(r.Values, v)
				}
			})

			result.Series = append(result.Series, &r)
			return nil
		})
		if err != nil {
			log.Println("Error iterating through results:", err)
		}
		resp.Results[i] = result
	}

	return json.NewEncoder(w).Encode(resp)
}


