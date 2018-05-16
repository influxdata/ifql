package functions_test

import (
	"testing"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/execute/executetest"
	"github.com/influxdata/ifql/query/querytest"
	"github.com/influxdata/ifql/semantic"
)

func TestCovariance_NewQuery(t *testing.T) {
	tests := []querytest.NewQueryTestCase{
		{
			Name: "simple covariance",
			Raw:  `from(db:"mydb") |> covariance(columns:["a","b"],)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "covariance1",
						Spec: &functions.CovarianceOpSpec{
							ValueLabel: execute.DefaultValueColLabel,
							AggregateConfig: execute.AggregateConfig{
								TimeValue: execute.DefaultStopColLabel,
								TimeCol:   execute.DefaultTimeColLabel,
								Columns:   []string{"a", "b"},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "covariance1"},
				},
			},
		},
		{
			Name: "pearsonr",
			Raw:  `from(db:"mydb")|>covariance(columns:["a","b"],pearsonr:true)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "covariance1",
						Spec: &functions.CovarianceOpSpec{
							ValueLabel:         execute.DefaultValueColLabel,
							PearsonCorrelation: true,
							AggregateConfig: execute.AggregateConfig{
								TimeValue: execute.DefaultStopColLabel,
								TimeCol:   execute.DefaultTimeColLabel,
								Columns:   []string{"a", "b"},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "covariance1"},
				},
			},
		},
		{
			Name: "global covariance",
			Raw:  `cov(x: from(db:"mydb"), y:from(db:"mydb"), on:["host"], pearsonr:true)`,
			Want: &query.Spec{
				Operations: []*query.Operation{
					{
						ID: "from0",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "from1",
						Spec: &functions.FromOpSpec{
							Database: "mydb",
						},
					},
					{
						ID: "join2",
						Spec: &functions.JoinOpSpec{
							On: []string{"host"},
							TableNames: map[query.OperationID]string{
								"from0": "x",
								"from1": "y",
							},
							Fn: &semantic.FunctionExpression{
								Params: []*semantic.FunctionParam{
									{Key: &semantic.Identifier{Name: "t"}},
								},
								Body: &semantic.ObjectExpression{
									Properties: []*semantic.Property{
										{
											Key: &semantic.Identifier{Name: "x"},
											Value: &semantic.MemberExpression{
												Object: &semantic.MemberExpression{
													Object:   &semantic.IdentifierExpression{Name: "t"},
													Property: "x",
												},
												Property: "_value",
											},
										},
										{
											Key: &semantic.Identifier{Name: "y"},
											Value: &semantic.MemberExpression{
												Object: &semantic.MemberExpression{
													Object:   &semantic.IdentifierExpression{Name: "t"},
													Property: "y",
												},
												Property: "_value",
											},
										},
									},
								},
							},
						},
					},
					{
						ID: "covariance3",
						Spec: &functions.CovarianceOpSpec{
							ValueLabel:         execute.DefaultValueColLabel,
							PearsonCorrelation: true,
							AggregateConfig: execute.AggregateConfig{
								TimeValue: execute.DefaultStopColLabel,
								TimeCol:   execute.DefaultTimeColLabel,
								Columns:   []string{"x", "y"},
							},
						},
					},
				},
				Edges: []query.Edge{
					{Parent: "from0", Child: "join2"},
					{Parent: "from1", Child: "join2"},
					{Parent: "join2", Child: "covariance3"},
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

func TestCovarianceOperation_Marshaling(t *testing.T) {
	data := []byte(`{
		"id":"covariance",
		"kind":"covariance",
		"spec":{
			"pearsonr":true
		}
	}`)
	op := &query.Operation{
		ID: "covariance",
		Spec: &functions.CovarianceOpSpec{
			PearsonCorrelation: true,
		},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestCovariance_Process(t *testing.T) {
	testCases := []struct {
		name string
		spec *functions.CovarianceProcedureSpec
		data []execute.Block
		want []*executetest.Block
	}{
		{
			name: "variance",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel: execute.DefaultValueColLabel,
				AggregateConfig: execute.AggregateConfig{
					TimeValue: execute.DefaultStopColLabel,
					TimeCol:   execute.DefaultTimeColLabel,
					Columns:   []string{"x", "y"},
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "x", Type: execute.TFloat},
					{Label: "y", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 5.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), 2.5},
				},
			}},
		},
		{
			name: "negative covariance",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel: execute.DefaultValueColLabel,
				AggregateConfig: execute.AggregateConfig{
					TimeValue: execute.DefaultStopColLabel,
					TimeCol:   execute.DefaultTimeColLabel,
					Columns:   []string{"x", "y"},
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "x", Type: execute.TFloat},
					{Label: "y", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 5.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 1.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), -2.5},
				},
			}},
		},
		{
			name: "small covariance",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel: execute.DefaultValueColLabel,
				AggregateConfig: execute.AggregateConfig{
					TimeValue: execute.DefaultStopColLabel,
					TimeCol:   execute.DefaultTimeColLabel,
					Columns:   []string{"x", "y"},
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "x", Type: execute.TFloat},
					{Label: "y", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 2.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), 0.5},
				},
			}},
		},
		{
			name: "pearson correlation",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel:         execute.DefaultValueColLabel,
				PearsonCorrelation: true,
				AggregateConfig: execute.AggregateConfig{
					TimeValue: execute.DefaultStopColLabel,
					TimeCol:   execute.DefaultTimeColLabel,
					Columns:   []string{"x", "y"},
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "x", Type: execute.TFloat},
					{Label: "y", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 1.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 5.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), 1.0},
				},
			}},
		},
		{
			name: "pearson correlation opposite",
			spec: &functions.CovarianceProcedureSpec{
				ValueLabel:         execute.DefaultValueColLabel,
				PearsonCorrelation: true,
				AggregateConfig: execute.AggregateConfig{
					TimeValue: execute.DefaultStopColLabel,
					TimeCol:   execute.DefaultTimeColLabel,
					Columns:   []string{"x", "y"},
				},
			},
			data: []execute.Block{&executetest.Block{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "x", Type: execute.TFloat},
					{Label: "y", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(0), 1.0, 5.0},
					{execute.Time(0), execute.Time(5), execute.Time(1), 2.0, 4.0},
					{execute.Time(0), execute.Time(5), execute.Time(2), 3.0, 3.0},
					{execute.Time(0), execute.Time(5), execute.Time(3), 4.0, 2.0},
					{execute.Time(0), execute.Time(5), execute.Time(4), 5.0, 1.0},
				},
			}},
			want: []*executetest.Block{{
				ColMeta: []execute.ColMeta{
					{Label: "_start", Type: execute.TTime, Key: true},
					{Label: "_stop", Type: execute.TTime, Key: true},
					{Label: "_time", Type: execute.TTime},
					{Label: "_value", Type: execute.TFloat},
				},
				Data: [][]interface{}{
					{execute.Time(0), execute.Time(5), execute.Time(5), -1.0},
				},
			}},
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			executetest.ProcessTestHelper(
				t,
				tc.data,
				tc.want,
				func(d execute.Dataset, c execute.BlockBuilderCache) execute.Transformation {
					return functions.NewCovarianceTransformation(d, c, tc.spec)
				},
			)
		})
	}
}
