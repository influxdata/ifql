package influxql

import (
	"encoding/json"
	"io"
	"log"
	"sort"
	"time"

	"github.com/influxdata/ifql/query/execute"
	"strconv"
	"github.com/pkg/errors"
	"fmt"
)

type Writer struct{}

type response struct {
	Results []Result `json:"results"`
	Err     string   `json:"err,omitempty"`
}

// WriteTo writes a collection of results to the influxdb 1.X http response format.
// Expectations/Assumptions:
//  1.  Each result will be published as a 'statement' in the top-level list of results.  The 'staementID'
//      will be interpreted as an integer, and will return an error otherwise.
//  2.  If the _field name is present in the tags, and a _value column is present, the _value column will
//      be renamed to the value of the _field tag
//  3.  If the _measurement name is present in the tags, it will be used as the row.Name for all rows.
//      Otherwise, we'll use the column value, which _must_ be present in that case.
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
		nameInt, err := strconv.Atoi(name)
		if err != nil {
			return fmt.Errorf("error converting result name to integer: %s", name)
		}
		result := Result{StatementID: nameInt}

		err = blocks.Do(func(b execute.Block) error {
			var r Row
			fieldName := ""
			measurementVaries := -1
			for k, v := range b.Tags() {
				if k == "_measurement" {
					r.Name = v
				} else if k == "_field" {
					fieldName = v
				} else {
					r.Tags[k] = v
				}
			}

			for i, c := range b.Cols() {
				if c.Label == "_time" {
					r.Columns = append(r.Columns, "time")
				} else if c.Label == "_value" && fieldName != "" {
					r.Columns = append(r.Columns, fieldName)
				} else if !c.Common {
					r.Columns = append(r.Columns, c.Label)
					if r.Name == "" && c.Label == "_measurement" {
						measurementVaries = i
					}
				}
			}
			if r.Name == "" && measurementVaries == -1 {
				return fmt.Errorf("no Measurement name found in result blocks for result: %s", name)
			}

			times := b.Times()
			var err2 error
			times.DoTime(func(ts []execute.Time, rr execute.RowReader) {
				for i := range ts {
					var v []interface{}

					for j, c := range rr.Cols() {
						if c.Common {
							continue
						}

						if j == measurementVaries {
							if c.Type != execute.TString {
								err2 = errors.New("unexpected type, _measurement is not a string")
							}
							r.Name = rr.AtString(i,j)
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
							v = append(v, rr.AtTime(i, j).Time().Format(time.RFC3339))
						default:
							v = append(v, "unknown")
						}
					}

					r.Values = append(r.Values, v)
				}
			})

			if err2 != nil {
				return err2
			}

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


