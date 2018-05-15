package functions_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/querytest"
)

func TestOutputHTTP_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "from with database with range",
			Raw:  `from(db:"mydb") |> outputHTTP(addr: "https://localhost:8081", method:"POST",  timeout: 50s)`, //[{header:"fred" value:"oh no"}, {header:"key2", "value":"oh my!"}, {header:"x-forwarded-for", value:"https://fakeaddr.com"}])`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "outputHTTP1",
						Spec: &functions.OutputHTTPOpSpec{
							Addr:       "https://localhost:8081",
							Method:     "POST",
							Timeout:    50 * time.Second,
							TimeColumn: execute.TimeColLabel,
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "outputHTTP1"},
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

func TestOutputHTTP_Process(t *testing.T) {
	data := []byte{}
	wg := sync.WaitGroup{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		fmt.Println("IN SERVER")
		defer wg.Done()
		data, err = ioutil.ReadAll(r.Body)
		fmt.Println("after readall ", string(data))
		if err != nil {
			t.Log(err)
			t.FailNow()
		}
	}))
	type wanted struct {
		Block  []*executetest.Block
		Result []byte
	}
	testCases := []struct {
		name string
		spec *functions.OutputHTTPProcedureSpec
		data []execute.Block
		want wanted
	}{
		{
			name: "one block",
			spec: &functions.OutputHTTPProcedureSpec{
				Spec: &functions.OutputHTTPOpSpec{
					Addr:       server.URL,
					Method:     "POST",
					Timeout:    50 * time.Second,
					TimeColumn: execute.TimeColLabel,
				},
			},
			data: []execute.Block{&executetest.Block{
				Bnds: execute.Bounds{
					Start: 1,
					Stop:  5,
				},
				ColMeta: []execute.ColMeta{
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(1), 2.0},
					{execute.Time(2), 1.0},
					{execute.Time(3), 3.0},
					{execute.Time(4), 4.0},
				},
			}},
			want: wanted{
				Block:  []*executetest.Block(nil),
				Result: []byte("food"),
			},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			wg.Add(1)
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want.Block,
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					return functions.NewOutputHTTPTransformation(d, c, tc.spec)
				},
			)
			wg.Wait() // wait till we are done getting the data back
			if string(data) != string(tc.want.Result) {
				t.Logf("expected %s, got %s", string(tc.want.Result), string(data))
				t.Fail()
			}
		})
	}
}

// func TestOutputHTTP_PushDown(t *testing.T) {
// 	spec := &functions.RangeProcedureSpec{
// 		Bounds: plan.BoundsSpec{
// 			Stop: query.Now,
// 		},
// 	}
// 	root := &plan.Procedure{
// 		Spec: new(functions.FromProcedureSpec),
// 	}
// 	want := &plan.Procedure{
// 		Spec: &functions.FromProcedureSpec{
// 			BoundsSet: true,
// 			Bounds: plan.BoundsSpec{
// 				Stop: query.Now,
// 			},
// 		},
// 	}

// 	plantest.PhysicalPlan_PushDown_TestHelper(t, spec, root, false, want)
// }

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
