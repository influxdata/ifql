package functions

import (
	"fmt"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
)

const DistinctKind = "distinct"

type DistinctOpSpec struct {
	Column string `json:"column"`
}

var distinctSignature = query.DefaultFunctionSignature()

func init() {
	distinctSignature.Params["column"] = semantic.String

	query.RegisterFunction(DistinctKind, createDistinctOpSpec, distinctSignature)
	query.RegisterOpSpec(DistinctKind, newDistinctOp)
	plan.RegisterProcedureSpec(DistinctKind, newDistinctProcedure, DistinctKind)
	execute.RegisterTransformation(DistinctKind, createDistinctTransformation)
}

func createDistinctOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(DistinctOpSpec)

	if col, ok, err := args.GetString("column"); err != nil {
		return nil, err
	} else if ok {
		spec.Column = col
	} else {
		spec.Column = execute.DefaultValueColLabel
	}

	return spec, nil
}

func newDistinctOp() query.OperationSpec {
	return new(DistinctOpSpec)
}

func (s *DistinctOpSpec) Kind() query.OperationKind {
	return DistinctKind
}

type DistinctProcedureSpec struct {
	Column string
}

func newDistinctProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*DistinctOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &DistinctProcedureSpec{
		Column: spec.Column,
	}, nil
}

func (s *DistinctProcedureSpec) Kind() plan.ProcedureKind {
	return DistinctKind
}
func (s *DistinctProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(DistinctProcedureSpec)

	*ns = *s

	return ns
}

func createDistinctTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*DistinctProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewDistinctTransformation(d, cache, s)
	return t, d, nil
}

type distinctTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache

	column string
}

func NewDistinctTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *DistinctProcedureSpec) *distinctTransformation {
	return &distinctTransformation{
		d:      d,
		cache:  cache,
		column: spec.Column,
	}
}

func (t *distinctTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *distinctTransformation) Process(id execute.DatasetID, b execute.Block) error {
	builder, created := t.cache.BlockBuilder(b.Key())
	if !created {
		return fmt.Errorf("distinct found duplicate block with key: %v", b.Key())
	}
	execute.AddBlockCols(b, builder)

	colIdx := execute.ColIdx(t.column, builder.Cols())
	if colIdx < 0 {
		return fmt.Errorf("no column %q exists", t.column)
	}
	col := builder.Cols()[colIdx]

	var (
		boolDistinct   map[bool]bool
		intDistinct    map[int64]bool
		uintDistinct   map[uint64]bool
		floatDistinct  map[float64]bool
		stringDistinct map[string]bool
		timeDistinct   map[execute.Time]bool
	)
	switch col.Type {
	case execute.TBool:
		boolDistinct = make(map[bool]bool)
	case execute.TInt:
		intDistinct = make(map[int64]bool)
	case execute.TUInt:
		uintDistinct = make(map[uint64]bool)
	case execute.TFloat:
		floatDistinct = make(map[float64]bool)
	case execute.TString:
		stringDistinct = make(map[string]bool)
	case execute.TTime:
		timeDistinct = make(map[execute.Time]bool)
	}

	return b.Do(func(cr execute.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			// Check distinct
			switch col.Type {
			case execute.TBool:
				v := cr.Bools(colIdx)[i]
				if boolDistinct[v] {
					continue
				}
				boolDistinct[v] = true
			case execute.TInt:
				v := cr.Ints(colIdx)[i]
				if intDistinct[v] {
					continue
				}
				intDistinct[v] = true
			case execute.TUInt:
				v := cr.UInts(colIdx)[i]
				if uintDistinct[v] {
					continue
				}
				uintDistinct[v] = true
			case execute.TFloat:
				v := cr.Floats(colIdx)[i]
				if floatDistinct[v] {
					continue
				}
				floatDistinct[v] = true
			case execute.TString:
				v := cr.Strings(colIdx)[i]
				if stringDistinct[v] {
					continue
				}
				stringDistinct[v] = true
			case execute.TTime:
				v := cr.Times(colIdx)[i]
				if timeDistinct[v] {
					continue
				}
				timeDistinct[v] = true
			}

			execute.AppendRecord(i, cr, builder)
		}
		return nil
	})
}

func (t *distinctTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *distinctTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *distinctTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
