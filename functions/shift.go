package functions

import (
	"fmt"

	"github.com/influxdata/ifql/interpreter"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
)

const ShiftKind = "shift"

type ShiftOpSpec struct {
	Shift   query.Duration `json:"shift"`
	Columns []string       `json:"columns"`
}

var shiftSignature = query.DefaultFunctionSignature()

func init() {
	shiftSignature.Params["shift"] = semantic.Duration

	query.RegisterFunction(ShiftKind, createShiftOpSpec, shiftSignature)
	query.RegisterOpSpec(ShiftKind, newShiftOp)
	plan.RegisterProcedureSpec(ShiftKind, newShiftProcedure, ShiftKind)
	execute.RegisterTransformation(ShiftKind, createShiftTransformation)
}

func createShiftOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}

	spec := new(ShiftOpSpec)

	if shift, err := args.GetRequiredDuration("shift"); err != nil {
		return nil, err
	} else {
		spec.Shift = shift
	}

	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return nil, err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return nil, err
		}
		spec.Columns = columns
	} else {
		spec.Columns = []string{
			execute.DefaultTimeColLabel,
			execute.DefaultStopColLabel,
			execute.DefaultStartColLabel,
		}
	}
	return spec, nil
}

func newShiftOp() query.OperationSpec {
	return new(ShiftOpSpec)
}

func (s *ShiftOpSpec) Kind() query.OperationKind {
	return ShiftKind
}

type ShiftProcedureSpec struct {
	Shift   query.Duration
	Columns []string
}

func newShiftProcedure(qs query.OperationSpec, _ plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*ShiftOpSpec)
	if !ok {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}

	return &ShiftProcedureSpec{
		Shift:   spec.Shift,
		Columns: spec.Columns,
	}, nil
}

func (s *ShiftProcedureSpec) Kind() plan.ProcedureKind {
	return ShiftKind
}

func (s *ShiftProcedureSpec) Copy() plan.ProcedureSpec {
	ns := new(ShiftProcedureSpec)
	*ns = *s

	if s.Columns != nil {
		ns.Columns = make([]string, len(s.Columns))
		copy(ns.Columns, s.Columns)
	}
	return ns
}

func createShiftTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*ShiftProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewShiftTransformation(d, cache, s)
	return t, d, nil
}

type shiftTransformation struct {
	d       execute.Dataset
	cache   execute.BlockBuilderCache
	shift   execute.Duration
	columns []string
}

func NewShiftTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *ShiftProcedureSpec) *shiftTransformation {
	return &shiftTransformation{
		d:       d,
		cache:   cache,
		shift:   execute.Duration(spec.Shift),
		columns: spec.Columns,
	}
}

func (t *shiftTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) error {
	return t.d.RetractBlock(key)
}

func (t *shiftTransformation) Process(id execute.DatasetID, b execute.Block) error {
	key := b.Key()
	// Update key
	cols := make([]execute.ColMeta, len(key.Cols()))
	values := make([]interface{}, len(key.Cols()))
	for j, c := range key.Cols() {
		if execute.ContainsStr(t.columns, c.Label) {
			if c.Type != execute.TTime {
				return fmt.Errorf("column %q is not of type time", c.Label)
			}
			cols[j] = c
			values[j] = key.ValueTime(j).Add(t.shift)
		} else {
			cols[j] = c
			values[j] = key.Value(j)
		}
	}
	key = execute.NewPartitionKey(cols, values)

	builder, created := t.cache.BlockBuilder(key)
	if !created {
		return fmt.Errorf("shift found duplicate block with key: %v", b.Key())
	}
	execute.AddBlockCols(b, builder)

	return b.Do(func(cr execute.ColReader) error {
		for j, c := range cr.Cols() {
			if execute.ContainsStr(t.columns, c.Label) {
				l := cr.Len()
				for i := 0; i < l; i++ {
					builder.AppendTime(j, cr.Times(j)[i].Add(t.shift))
				}
			} else {
				execute.AppendCol(j, j, cr, builder)
			}
		}
		return nil
	})
}

func (t *shiftTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
	return t.d.UpdateWatermark(mark)
}

func (t *shiftTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}

func (t *shiftTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}

func (t *shiftTransformation) SetParents(ids []execute.DatasetID) {}
