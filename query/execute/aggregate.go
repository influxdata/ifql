package execute

import (
	"fmt"

	"github.com/influxdata/ifql/interpreter"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/semantic"
	"github.com/pkg/errors"
)

type aggregateTransformation struct {
	d     Dataset
	cache BlockBuilderCache
	agg   Aggregate

	config AggregateConfig
}

type AggregateConfig struct {
	Columns   []string `json:"columns"`
	TimeValue string   `json:"time_value"`
	TimeCol   string   `json:"time_col"`
}

func (c AggregateConfig) Copy() AggregateConfig {
	nc := c
	if c.Columns != nil {
		nc.Columns = make([]string, len(c.Columns))
		copy(nc.Columns, c.Columns)
	}
	return nc
}

func (c *AggregateConfig) ReadArgs(args query.Arguments) error {
	if timeCol, ok, err := args.GetString("timeCol"); err != nil {
		return err
	} else if ok {
		c.TimeCol = timeCol
	} else {
		c.TimeCol = DefaultTimeColLabel
	}

	if timeValue, ok, err := args.GetString("timeValue"); err != nil {
		return err
	} else if ok {
		c.TimeValue = timeValue
	} else {
		c.TimeValue = DefaultStopColLabel
	}

	if cols, ok, err := args.GetArray("columns", semantic.String); err != nil {
		return err
	} else if ok {
		columns, err := interpreter.ToStringArray(cols)
		if err != nil {
			return err
		}
		c.Columns = columns
	} else {
		c.Columns = []string{DefaultValueColLabel}
	}
	return nil
}

func NewAggregateTransformation(d Dataset, c BlockBuilderCache, agg Aggregate, config AggregateConfig) *aggregateTransformation {
	return &aggregateTransformation{
		d:      d,
		cache:  c,
		agg:    agg,
		config: config,
	}
}

func NewAggregateTransformationAndDataset(id DatasetID, mode AccumulationMode, agg Aggregate, config AggregateConfig, a *Allocator) (*aggregateTransformation, Dataset) {
	cache := NewBlockBuilderCache(a)
	d := NewDataset(id, mode, cache)
	return NewAggregateTransformation(d, cache, agg, config), d
}

func (t *aggregateTransformation) RetractBlock(id DatasetID, key PartitionKey) error {
	//TODO(nathanielc): Store intermediate state for retractions
	return t.d.RetractBlock(key)
}

func (t *aggregateTransformation) Process(id DatasetID, b Block) error {
	builder, new := t.cache.BlockBuilder(b.Key())
	if !new {
		return fmt.Errorf("found duplicate block with key: %v", b.Key())
	}

	builder.AddCol(TimeCol)
	AddBlockKeyCols(b, builder)

	builderColMap := make([]int, len(t.config.Columns))
	blockColMap := make([]int, len(t.config.Columns))
	aggregates := make([]ValueFunc, len(t.config.Columns))

	cols := b.Cols()
	for j, label := range t.config.Columns {
		idx := -1
		for bj, bc := range cols {
			if bc.Label == label {
				idx = bj
				break
			}
		}
		if idx < 0 {
			return fmt.Errorf("column %q does not exist", label)
		}
		c := cols[idx]
		if c.Key {
			return errors.New("cannot aggregate columns that are part of the partition key")
		}
		var vf ValueFunc
		switch c.Type {
		case TBool:
			vf = t.agg.NewBoolAgg()
		case TInt:
			vf = t.agg.NewIntAgg()
		case TUInt:
			vf = t.agg.NewUIntAgg()
		case TFloat:
			vf = t.agg.NewFloatAgg()
		case TString:
			vf = t.agg.NewStringAgg()
		default:
			return fmt.Errorf("unsupported aggregate column type %v", c.Type)
		}
		aggregates[j] = vf
		builderColMap[j] = builder.AddCol(ColMeta{
			Label: c.Label,
			Type:  vf.Type(),
		})
		blockColMap[j] = idx
	}

	// Add row for aggregate values
	timeIdx := ColIdx(t.config.TimeValue, b.Key().Cols())
	if timeIdx < 0 {
		return fmt.Errorf("timeValue column %q does not exist", t.config.TimeValue)
	}
	timeCol := b.Key().Cols()[timeIdx]
	if timeCol.Type != TTime {
		return fmt.Errorf("timeValue column %q does not have type time", t.config.TimeValue)
	}
	builder.AppendTime(0, b.Key().ValueTime(timeIdx))

	b.Do(func(cr ColReader) error {
		for j := range t.config.Columns {
			c := builder.Cols()[builderColMap[j]]
			vf := aggregates[j]

			tj := blockColMap[j]

			switch c.Type {
			case TBool:
				vf.(DoBoolAgg).DoBool(cr.Bools(tj))
			case TInt:
				vf.(DoIntAgg).DoInt(cr.Ints(tj))
			case TUInt:
				vf.(DoUIntAgg).DoUInt(cr.UInts(tj))
			case TFloat:
				vf.(DoFloatAgg).DoFloat(cr.Floats(tj))
			case TString:
				vf.(DoStringAgg).DoString(cr.Strings(tj))
			default:
				return fmt.Errorf("unsupport aggregate type %v", c.Type)
			}
		}
		return nil
	})
	for j, vf := range aggregates {
		bj := builderColMap[j]
		// Append aggregated value
		switch vf.Type() {
		case TBool:
			builder.AppendBool(bj, vf.(BoolValueFunc).ValueBool())
		case TInt:
			builder.AppendInt(bj, vf.(IntValueFunc).ValueInt())
		case TUInt:
			builder.AppendUInt(bj, vf.(UIntValueFunc).ValueUInt())
		case TFloat:
			builder.AppendFloat(bj, vf.(FloatValueFunc).ValueFloat())
		case TString:
			builder.AppendString(bj, vf.(StringValueFunc).ValueString())
		}
	}
	return nil
}

func (t *aggregateTransformation) UpdateWatermark(id DatasetID, mark Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *aggregateTransformation) UpdateProcessingTime(id DatasetID, pt Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *aggregateTransformation) Finish(id DatasetID, err error) {
	t.d.Finish(err)
}

type Aggregate interface {
	NewBoolAgg() DoBoolAgg
	NewIntAgg() DoIntAgg
	NewUIntAgg() DoUIntAgg
	NewFloatAgg() DoFloatAgg
	NewStringAgg() DoStringAgg
}

type ValueFunc interface {
	Type() DataType
}
type DoBoolAgg interface {
	ValueFunc
	DoBool([]bool)
}
type DoFloatAgg interface {
	ValueFunc
	DoFloat([]float64)
}
type DoIntAgg interface {
	ValueFunc
	DoInt([]int64)
}
type DoUIntAgg interface {
	ValueFunc
	DoUInt([]uint64)
}
type DoStringAgg interface {
	ValueFunc
	DoString([]string)
}

type BoolValueFunc interface {
	ValueBool() bool
}
type FloatValueFunc interface {
	ValueFloat() float64
}
type IntValueFunc interface {
	ValueInt() int64
}
type UIntValueFunc interface {
	ValueUInt() uint64
}
type StringValueFunc interface {
	ValueString() string
}
