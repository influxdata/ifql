package execute

import "github.com/influxdata/ifql/query"

type aggregateTransformation struct {
	d     Dataset
	cache BlockBuilderCache
	agg   Aggregate

	config AggregateConfig
}

type AggregateConfig struct {
	UseStartTime bool `json:"useStartTime"`
}

func (c *AggregateConfig) ReadArgs(args query.Arguments) error {
	if useStartTime, ok, err := args.GetBool("useStartTime"); err != nil {
		return err
	} else if ok {
		c.UseStartTime = useStartTime
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

func (t *aggregateTransformation) RetractBlock(id DatasetID, meta BlockMetadata) error {
	//TODO(nathanielc): Store intermediate state for retractions
	key := ToBlockKey(meta)
	return t.d.RetractBlock(key)
}

func (t *aggregateTransformation) Process(id DatasetID, b Block) error {
	builder, new := t.cache.BlockBuilder(b)
	if new {
		cols := b.Cols()
		for j, c := range cols {
			switch c.Kind {
			case TimeColKind:
				builder.AddCol(c)
			case TagColKind:
				if c.Common {
					builder.AddCol(c)
					builder.SetCommonString(j, b.Tags()[c.Label])
				}
			case ValueColKind:
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
				}
				builder.AddCol(ColMeta{
					Label: c.Label,
					Type:  vf.Type(),
					Kind:  ValueColKind,
				})
			}
		}
	}
	// Add row for aggregate values
	timeIdx := TimeIdx(builder.Cols())
	if t.config.UseStartTime {
		builder.AppendTime(timeIdx, b.Bounds().Start)
	} else {
		builder.AppendTime(timeIdx, b.Bounds().Stop)
	}

	for j, c := range b.Cols() {
		if c.Kind != ValueColKind {
			continue
		}

		// TODO(nathanielc): This reads the block multiple times (once per value column), is that OK?
		values := b.Col(j)
		var vf ValueFunc
		switch c.Type {
		case TBool:
			f := t.agg.NewBoolAgg()
			values.DoBool(func(vs []bool, _ RowReader) {
				f.DoBool(vs)
			})
			vf = f
		case TInt:
			f := t.agg.NewIntAgg()
			values.DoInt(func(vs []int64, _ RowReader) {
				f.DoInt(vs)
			})
			vf = f
		case TUInt:
			f := t.agg.NewUIntAgg()
			values.DoUInt(func(vs []uint64, _ RowReader) {
				f.DoUInt(vs)
			})
			vf = f
		case TFloat:
			f := t.agg.NewFloatAgg()
			values.DoFloat(func(vs []float64, _ RowReader) {
				f.DoFloat(vs)
			})
			vf = f
		case TString:
			f := t.agg.NewStringAgg()
			values.DoString(func(vs []string, _ RowReader) {
				f.DoString(vs)
			})
			vf = f
		}
		switch vf.Type() {
		case TBool:
			v := vf.(BoolValueFunc)
			builder.AppendBool(j, v.ValueBool())
		case TInt:
			v := vf.(IntValueFunc)
			builder.AppendInt(j, v.ValueInt())
		case TUInt:
			v := vf.(UIntValueFunc)
			builder.AppendUInt(j, v.ValueUInt())
		case TFloat:
			v := vf.(FloatValueFunc)
			builder.AppendFloat(j, v.ValueFloat())
		case TString:
			v := vf.(StringValueFunc)
			builder.AppendString(j, v.ValueString())
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
