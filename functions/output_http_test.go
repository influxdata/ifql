package functions_test

import (
	"testing"
	"time"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/query/plan/plantest"
	"github.com/influxdata/ifql/query/querytest"
)

// func TestOutputHTTP_Marshaling(t *testing.T) {
// 	data := []byte(`{"id":"outputHTTP", "kind":"outputHTTP"}`)
// 	op := &query.Operation{
// 		ID:   "outputHTTP",
// 		Spec: &functions.OutputHTTPOpSpec{},
// 	}
// 	querytest.OperationMarshalingTestHelper(t, data, op)
// }

// func TestOutputHTTP_NewQuery(t *testing.T) {
// 	executetest.TransformationPassThroughTestHelper(t, func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {

// 	})
// }

// package functions_test

// import (
// 	"testing"
// 	"time"

// 	"github.com/influxdata/ifql/functions"
// 	"github.com/influxdata/ifql/query"
// 	"github.com/influxdata/ifql/query/plan"
// 	"github.com/influxdata/ifql/query/plan/plantest"
// 	"github.com/influxdata/ifql/query/querytest"
// )

func TestOutputHTTP_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with database with range",
			Raw:  `from(db:"mydb") |> range(start:-4h, stop:-2h) |> sum()`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "range1",
						Spec: &functions.RangeOpSpec{
							Start: query.Time{
								Relative:   -4 * time.Hour,
								IsRelative: true,
							},
							Stop: query.Time{
								Relative:   -2 * time.Hour,
								IsRelative: true,
							},
						},
					},
					{
						ID:   "sum2",
						Spec: &functions.SumOpSpec{},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "range1"},
					{Parent: "range1", Child: "sum2"},
				},
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			querytest.NewQueryTestHelper(t, tc)
		})
	}
}

func TestOutputHTTPOpSpec_UnmarshalJSON(t *testing.T) {
	type fields struct {
		Addr        string
		Method      string
		Headers     map[string]string
		URLParams   map[string]string
		Timeout     time.Duration
		NoKeepAlive bool
	}
	tests := []struct {
		name    string
		fields  fields
		bytes   []byte
		wantErr bool
	}{
		{
			name: "happy path",
			bytes: []byte(`
			{
				"id": "outputHTTP",
				"kind": "outputHTTP",
				"spec": {
				  "addr": "https://localhost:8081",
				  "method" :"POST"
				}
			}`),
			fields: fields{
				Addr:   "https://localhost:8081",
				Method: "POST",
			},
		}, {
			name: "bad address",
			bytes: []byte(`
		{
			"id": "outputHTTP",
			"kind": "outputHTTP",
			"spec": {
			  "addr": "https://loc	alhost:8081",
			  "method" :"POST"
			}
		}`),
			fields: fields{
				Addr:   "https://localhost:8081",
				Method: "POST",
			},
			wantErr: true,
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &functions.OutputHTTPOpSpec{
				Addr:        tt.fields.Addr,
				Method:      tt.fields.Method,
				Headers:     tt.fields.Headers,
				URLParams:   tt.fields.URLParams,
				Timeout:     tt.fields.Timeout,
				NoKeepAlive: tt.fields.NoKeepAlive,
			}
			op := &query.Operation{
				ID:   "outputHTTP",
				Spec: o,
			}
			if !tt.wantErr {
				querytest.OperationMarshalingTestHelper(t, tt.bytes, op)
			} else if err := o.UnmarshalJSON(tt.bytes); err == nil {
				t.Errorf("OutputHTTPOpSpec.UnmarshalJSON() error = %v, wantErr %v for test %s", err, tt.wantErr, tt.name)
			}
		})
	}
}

func TestOutputHTTP_PushDown(t *testing.T) {
	spec := &functions.RangeProcedureSpec{
		Bounds: plan.BoundsSpec{
			Stop: query.Now,
		},
	}
	root := &plan.Procedure{
		Spec: new(functions.FromProcedureSpec),
	}
	want := &plan.Procedure{
		Spec: &functions.FromProcedureSpec{
			BoundsSet: true,
			Bounds: plan.BoundsSpec{
				Stop: query.Now,
			},
		},
	}

	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
}

// func TestOutputHTTP_PushDown_Duplicate(t *testing.T) {
// 	spec := &functions.RangeProcedureSpec{
// 		Bounds: plan.BoundsSpec{
// 			Stop: query.Now,
// 		},
// 	}
// 	root := &plan.Procedure{
// 		Spec: &functions.FromProcedureSpec{
// 			BoundsSet: true,
// 			Bounds: plan.BoundsSpec{
// 				Start: query.MinTime,
// 				Stop:  query.Now,
// 			},
// 		},
// 	}
// 	want := &plan.Procedure{
// 		// Expect the duplicate has been reset to zero values
// 		Spec: new(functions.FromProcedureSpec),
// 	}
// }
