package functions

import (
	"fmt"

	"github.com/influxdata/ifql/interpreter"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
)

const CumulativeSumKind = "cumulativeSum"

type CumulativeSumOpSpec struct {
	Columns []string `json:"columns"`
}

var cumulativeSumSignature = query.DefaultFunctionSignature()

func init() {
	query.RegisterFunction(CumulativeSumKind, createCumulativeSumOpSpec, cumulativeSumSignature)
	query.RegisterOpSpec(CumulativeSumKind, newCumulativeSumOp)
	plan.RegisterProcedureSpec(CumulativeSumKind, newCumulativeSumProcedure, CumulativeSumKind)
	execute.RegisterTransformation(CumulativeSumKind, createCumulativeSumTransformation)
}

func createCumulativeSumOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(CumulativeSumOpSpec)
	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return nil, err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return nil, err
		}
		spec.Columns = columns
	} else {
		spec.Columns = []string{execute.DefaultValueColLabel}
	}
	return spec, nil
}

func newCumulativeSumOp() query.OperationSpec {
	return new(CumulativeSumOpSpec)
}

func (s *CumulativeSumOpSpec) Kind() query.OperationKind {
	return CumulativeSumKind
}

type CumulativeSumProcedureSpec struct {
	Columns []string
}

func newCumulativeSumProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*CumulativeSumOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &CumulativeSumProcedureSpec{
		Columns: spec.Columns,
	}, nil
}

func (s *CumulativeSumProcedureSpec) Kind() plan.ProcedureKind {
	return CumulativeSumKind
}
func (s *CumulativeSumProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(CumulativeSumProcedureSpec)
	*ns = *s
	if s.Columns != nil {
		ns.Columns = make([]string, len(s.Columns))
		copy(ns.Columns, s.Columns)
	}
	return ns
}

func createCumulativeSumTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*CumulativeSumProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewCumulativeSumTransformation(d, cache, s)
	return t, d, nil
}

type cumulativeSumTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache
	spec  CumulativeSumProcedureSpec
}

func NewCumulativeSumTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *CumulativeSumProcedureSpec) *cumulativeSumTransformation {
	return &cumulativeSumTransformation{
		d:     d,
		cache: cache,
		spec:  *spec,
	}
}

func (t *cumulativeSumTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *cumulativeSumTransformation) Process(id execute.DatasetID, b execute.Block) error {
	builder, created := t.cache.BlockBuilder(b.Key())
	if !created {
		return fmt.Errorf("cumulative sum found duplicate block with key: %v", b.Key())
	}
	execute.AddBlockCols(b, builder)

	cols := b.Cols()
	sumers := make([]*cumulativeSum, len(cols))
	for j, c := range cols {
		for _, label := range t.spec.Columns {
			if c.Label == label {
				sumers[j] = &cumulativeSum{}
				break
			}
		}
	}
	return b.Do(func(cr execute.ColReader) error {
		l := cr.Len()
		for j, c := range cols {
			switch c.Type {
			case execute.TBool:
				builder.AppendBools(j, cr.Bools(j))
			case execute.TInt:
				if sumers[j] != nil {
					for i := 0; i < l; i++ {
						builder.AppendInt(j, sumers[j].sumInt(cr.Ints(j)[i]))
					}
				} else {
					builder.AppendInts(j, cr.Ints(j))
				}
			case execute.TUInt:
				if sumers[j] != nil {
					for i := 0; i < l; i++ {
						builder.AppendUInt(j, sumers[j].sumUInt(cr.UInts(j)[i]))
					}
				} else {
					builder.AppendUInts(j, cr.UInts(j))
				}
			case execute.TFloat:
				if sumers[j] != nil {
					for i := 0; i < l; i++ {
						builder.AppendFloat(j, sumers[j].sumFloat(cr.Floats(j)[i]))
					}
				} else {
					builder.AppendFloats(j, cr.Floats(j))
				}
			case execute.TString:
				builder.AppendStrings(j, cr.Strings(j))
			case execute.TTime:
				builder.AppendTimes(j, cr.Times(j))
			}
		}
		return nil
	})
}

func (t *cumulativeSumTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *cumulativeSumTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *cumulativeSumTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}

type cumulativeSum struct {
	intVal   int64
	uintVal  uint64
	floatVal float64
}

func (s *cumulativeSum) sumInt(val int64) int64 {
	s.intVal += val
	return s.intVal
}

func (s *cumulativeSum) sumUInt(val uint64) uint64 {
	s.uintVal += val
	return s.uintVal
}

func (s *cumulativeSum) sumFloat(val float64) float64 {
	s.floatVal += val
	return s.floatVal
}
