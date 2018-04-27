package execute

import (
	"fmt"

	"github.com/influxdata/ifql/query"
	"github.com/pkg/errors"
)

type selectorTransformation struct {
	d     Dataset
	cache BlockBuilderCache

	config SelectorConfig
}

type SelectorConfig struct {
	Column       string `json:"column"`
	UseRowTime   bool   `json:"useRowTime"`
	UseStartTime bool   `json:"useStartTime"`
}

func (c *SelectorConfig) ReadArgs(args query.Arguments) error {
	if col, ok, err := args.GetString("column"); err != nil {
		return err
	} else if ok {
		c.Column = col
	}
	if useRowTime, ok, err := args.GetBool("useRowTime"); err != nil {
		return err
	} else if ok {
		c.UseRowTime = useRowTime
	}
	if useStartTime, ok, err := args.GetBool("useStartTime"); err != nil {
		return err
	} else if ok {
		c.UseStartTime = useStartTime
	}
	if c.UseStartTime && c.UseRowTime {
		return errors.New("cannot use both row time and start time")
	}
	return nil
}

type rowSelectorTransformation struct {
	selectorTransformation
	selector RowSelector
}
type indexSelectorTransformation struct {
	selectorTransformation
	selector IndexSelector
}

func NewRowSelectorTransformationAndDataset(id DatasetID, mode AccumulationMode, selector RowSelector, config SelectorConfig, a *Allocator) (*rowSelectorTransformation, Dataset) {
	cache := NewBlockBuilderCache(a)
	d := NewDataset(id, mode, cache)
	return NewRowSelectorTransformation(d, cache, selector, config), d
}
func NewRowSelectorTransformation(d Dataset, c BlockBuilderCache, selector RowSelector, config SelectorConfig) *rowSelectorTransformation {
	return &rowSelectorTransformation{
		selectorTransformation: newSelectorTransformation(d, c, config),
		selector:               selector,
	}
}

func NewIndexSelectorTransformationAndDataset(id DatasetID, mode AccumulationMode, selector IndexSelector, config SelectorConfig, a *Allocator) (*indexSelectorTransformation, Dataset) {
	cache := NewBlockBuilderCache(a)
	d := NewDataset(id, mode, cache)
	return NewIndexSelectorTransformation(d, cache, selector, config), d
}
func NewIndexSelectorTransformation(d Dataset, c BlockBuilderCache, selector IndexSelector, config SelectorConfig) *indexSelectorTransformation {
	return &indexSelectorTransformation{
		selectorTransformation: newSelectorTransformation(d, c, config),
		selector:               selector,
	}
}

func newSelectorTransformation(d Dataset, c BlockBuilderCache, config SelectorConfig) selectorTransformation {
	if config.Column == "" {
		config.Column = DefaultValueColLabel
	}
	return selectorTransformation{
		d:      d,
		cache:  c,
		config: config,
	}
}

func (t *selectorTransformation) RetractBlock(id DatasetID, meta BlockMetadata) error {
	//TODO(nathanielc): Store intermediate state for retractions
	key := ToBlockKey(meta)
	return t.d.RetractBlock(key)
}
func (t *selectorTransformation) UpdateWatermark(id DatasetID, mark Time) error {
	return t.d.UpdateWatermark(mark)
}
func (t *selectorTransformation) UpdateProcessingTime(id DatasetID, pt Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *selectorTransformation) Finish(id DatasetID, err error) {
	t.d.Finish(err)
}

func (t *selectorTransformation) setupBuilder(b Block) (BlockBuilder, int) {
	builder, new := t.cache.BlockBuilder(b)
	if new {
		AddBlockCols(b, builder)
	}

	cols := builder.Cols()
	valueIdx := ColIdx(t.config.Column, cols)
	return builder, valueIdx
}

func (t *indexSelectorTransformation) Process(id DatasetID, b Block) error {
	builder, valueIdx := t.setupBuilder(b)
	valueCol := builder.Cols()[valueIdx]

	var newTime Time
	if t.config.UseStartTime {
		newTime = b.Bounds().Start
	} else {
		newTime = b.Bounds().Stop
	}

	values := b.Col(valueIdx)
	switch valueCol.Type {
	case TBool:
		s := t.selector.NewBoolSelector()
		values.DoBool(func(vs []bool, rr RowReader) {
			selected := s.DoBool(vs)
			t.appendSelected(selected, builder, rr, newTime)
		})
	case TInt:
		s := t.selector.NewIntSelector()
		values.DoInt(func(vs []int64, rr RowReader) {
			selected := s.DoInt(vs)
			t.appendSelected(selected, builder, rr, newTime)
		})
	case TUInt:
		s := t.selector.NewUIntSelector()
		values.DoUInt(func(vs []uint64, rr RowReader) {
			selected := s.DoUInt(vs)
			t.appendSelected(selected, builder, rr, newTime)
		})
	case TFloat:
		s := t.selector.NewFloatSelector()
		values.DoFloat(func(vs []float64, rr RowReader) {
			selected := s.DoFloat(vs)
			t.appendSelected(selected, builder, rr, newTime)
		})
	case TString:
		s := t.selector.NewStringSelector()
		values.DoString(func(vs []string, rr RowReader) {
			selected := s.DoString(vs)
			t.appendSelected(selected, builder, rr, newTime)
		})
	}
	return nil
}

func (t *rowSelectorTransformation) Process(id DatasetID, b Block) error {
	builder, valueIdx := t.setupBuilder(b)
	if valueIdx < 0 {
		return fmt.Errorf("no column %q exists", t.config.Column)
	}
	valueCol := builder.Cols()[valueIdx]

	values := b.Col(valueIdx)
	var rower Rower
	switch valueCol.Type {
	case TBool:
		s := t.selector.NewBoolSelector()
		values.DoBool(s.DoBool)
		rower = s
	case TInt:
		s := t.selector.NewIntSelector()
		values.DoInt(s.DoInt)
		rower = s
	case TUInt:
		s := t.selector.NewUIntSelector()
		values.DoUInt(s.DoUInt)
		rower = s
	case TFloat:
		s := t.selector.NewFloatSelector()
		values.DoFloat(s.DoFloat)
		rower = s
	case TString:
		s := t.selector.NewStringSelector()
		values.DoString(s.DoString)
		rower = s
	}

	rows := rower.Rows()
	var newTime Time
	if t.config.UseStartTime {
		newTime = b.Bounds().Start
	} else {
		newTime = b.Bounds().Stop
	}
	t.appendRows(builder, rows, newTime)
	return nil
}

func (t *indexSelectorTransformation) appendSelected(selected []int, builder BlockBuilder, rr RowReader, newTime Time) {
	if len(selected) == 0 {
		return
	}
	cols := builder.Cols()
	for j, c := range cols {
		for _, i := range selected {
			switch c.Type {
			case TBool:
				builder.AppendBool(j, rr.AtBool(i, j))
			case TInt:
				builder.AppendInt(j, rr.AtInt(i, j))
			case TUInt:
				builder.AppendUInt(j, rr.AtUInt(i, j))
			case TFloat:
				builder.AppendFloat(j, rr.AtFloat(i, j))
			case TString:
				builder.AppendString(j, rr.AtString(i, j))
			case TTime:
				nt := newTime
				if t.config.UseRowTime {
					nt = rr.AtTime(i, j)
				}
				builder.AppendTime(j, nt)
			default:
				PanicUnknownType(c.Type)
			}
		}
	}
}

func (t *rowSelectorTransformation) appendRows(builder BlockBuilder, rows []Row, newTime Time) {
	cols := builder.Cols()
	for j, c := range cols {
		for _, row := range rows {
			v := row.Values[j]
			switch c.Type {
			case TBool:
				builder.AppendBool(j, v.(bool))
			case TInt:
				builder.AppendInt(j, v.(int64))
			case TUInt:
				builder.AppendUInt(j, v.(uint64))
			case TFloat:
				builder.AppendFloat(j, v.(float64))
			case TString:
				builder.AppendString(j, v.(string))
			case TTime:
				if t.config.UseRowTime {
					builder.AppendTime(j, v.(Time))
				} else {
					builder.AppendTime(j, newTime)
				}
			default:
				PanicUnknownType(c.Type)
			}
		}
	}
}

type IndexSelector interface {
	NewBoolSelector() DoBoolIndexSelector
	NewIntSelector() DoIntIndexSelector
	NewUIntSelector() DoUIntIndexSelector
	NewFloatSelector() DoFloatIndexSelector
	NewStringSelector() DoStringIndexSelector
}
type DoBoolIndexSelector interface {
	DoBool([]bool) []int
}
type DoIntIndexSelector interface {
	DoInt([]int64) []int
}
type DoUIntIndexSelector interface {
	DoUInt([]uint64) []int
}
type DoFloatIndexSelector interface {
	DoFloat([]float64) []int
}
type DoStringIndexSelector interface {
	DoString([]string) []int
}

type RowSelector interface {
	NewBoolSelector() DoBoolRowSelector
	NewIntSelector() DoIntRowSelector
	NewUIntSelector() DoUIntRowSelector
	NewFloatSelector() DoFloatRowSelector
	NewStringSelector() DoStringRowSelector
}

type Rower interface {
	Rows() []Row
}

type DoBoolRowSelector interface {
	Rower
	// What if the selector doesn't know yet and needs to wait all is finalized?
	DoBool(vs []bool, rr RowReader)
}
type DoIntRowSelector interface {
	Rower
	DoInt(vs []int64, rr RowReader)
}
type DoUIntRowSelector interface {
	Rower
	DoUInt(vs []uint64, rr RowReader)
}
type DoFloatRowSelector interface {
	Rower
	DoFloat(vs []float64, rr RowReader)
}
type DoStringRowSelector interface {
	Rower
	DoString(vs []string, rr RowReader)
}

type Row struct {
	Values []interface{}
}

func ReadRow(i int, rr RowReader) (row Row) {
	cols := rr.Cols()
	row.Values = make([]interface{}, len(cols))
	for j, c := range cols {
		switch c.Type {
		case TBool:
			row.Values[j] = rr.AtBool(i, j)
		case TInt:
			row.Values[j] = rr.AtInt(i, j)
		case TUInt:
			row.Values[j] = rr.AtUInt(i, j)
		case TFloat:
			row.Values[j] = rr.AtFloat(i, j)
		case TString:
			row.Values[j] = rr.AtString(i, j)
		case TTime:
			row.Values[j] = rr.AtTime(i, j)
		}
	}
	return
}
