package execute

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync/atomic"

	"github.com/influxdata/ifql/query"
	"github.com/pkg/errors"
)

const (
	DefaultStartColLabel = "_start"
	DefaultStopColLabel  = "_stop"
	DefaultTimeColLabel  = "_time"
	DefaultValueColLabel = "_value"
)

var TimeCol = ColMeta{
	Label: DefaultTimeColLabel,
	Type:  TTime,
}

type PartitionKey interface {
	Cols() []ColMeta

	ValueBool(j int) bool
	ValueUInt(j int) uint64
	ValueInt(j int) int64
	ValueFloat(j int) float64
	ValueString(j int) string
	ValueDuration(j int) Duration
	ValueTime(j int) Time

	Hash() uint64
	String() string
}

func PartitionKeyCompare(a, b PartitionKey) bool {
	aCols := a.Cols()
	bCols := b.Cols()
	if len(aCols) != len(bCols) {
		return false
	}
	for j, c := range aCols {
		if aCols[j] != bCols[j] {
			return false
		}
		switch c.Type {
		case TBool:
			if a.ValueBool(j) != b.ValueBool(j) {
				return false
			}
		case TInt:
			if a.ValueInt(j) != b.ValueInt(j) {
				return false
			}
		case TUInt:
			if a.ValueUInt(j) != b.ValueUInt(j) {
				return false
			}
		case TFloat:
			if a.ValueFloat(j) != b.ValueFloat(j) {
				return false
			}
		case TString:
			if a.ValueString(j) != b.ValueString(j) {
				return false
			}
		case TTime:
			if a.ValueTime(j) != b.ValueTime(j) {
				return false
			}
		}
	}
	return true
}

type partitionKey struct {
	cols    []ColMeta
	values  []interface{}
	hasHash bool
	hash    uint64
}

func (k *partitionKey) Cols() []ColMeta {
	return k.cols
}
func (k *partitionKey) ValueBool(j int) bool {
	return k.values[j].(bool)
}
func (k *partitionKey) ValueUInt(j int) uint64 {
	return k.values[j].(uint64)
}
func (k *partitionKey) ValueInt(j int) int64 {
	return k.values[j].(int64)
}
func (k *partitionKey) ValueFloat(j int) float64 {
	return k.values[j].(float64)
}
func (k *partitionKey) ValueString(j int) string {
	return k.values[j].(string)
}
func (k *partitionKey) ValueDuration(j int) Duration {
	return k.values[j].(Duration)
}
func (k *partitionKey) ValueTime(j int) Time {
	return k.values[j].(Time)
}

func (k *partitionKey) Hash() uint64 {
	if !k.hasHash {
		k.hasHash = true
		k.hash = computeKeyHash(k)
	}
	return k.hash
}

func (k partitionKey) String() string {
	panic("not implemented")
}

type Block interface {
	Key() PartitionKey

	Cols() []ColMeta

	// Do calls f to process the data contained within the block.
	// The function f will be called zero or more times.
	Do(f func(ColReader) error) error

	// RefCount modifies the reference count on the block by n.
	// When the RefCount goes to zero, the block is freed.
	RefCount(n int)
}

// OneTimeBlock is a Block that permits reading data only once.
// Specifically the ValueIterator may only be consumed once from any of the columns.
type OneTimeBlock interface {
	Block
	onetime()
}

// CacheOneTimeBlock returns a block that can be read multiple times.
// If the block is not a OneTimeBlock it is returned directly.
// Otherwise its contents are read into a new block.
func CacheOneTimeBlock(b Block, a *Allocator) Block {
	_, ok := b.(OneTimeBlock)
	if !ok {
		return b
	}
	return CopyBlock(b, a)
}

// CopyBlock returns a copy of the block and is OneTimeBlock safe.
func CopyBlock(b Block, a *Allocator) Block {
	builder := NewColListBlockBuilder(a)

	cols := b.Cols()
	colMap := make([]int, len(cols))
	for j, c := range cols {
		colMap[j] = j
		builder.AddCol(c)
		// TODO Set Common for any type
		//if c.IsTag() && c.Common {
		//	builder.SetCommonString(j, b.Tags()[c.Label])
		//}
	}

	AppendBlock(b, builder, colMap)
	// ColListBlockBuilders do not error
	nb, _ := builder.Block()
	return nb
}

// AddBlockCols adds the columns of b onto builder.
func AddBlockCols(b Block, builder BlockBuilder) {
	cols := b.Cols()
	for _, c := range cols {
		builder.AddCol(c)
		// TODO Set Common for any type
		//if c.IsTag() && c.Common {
		//	builder.SetCommonString(j, b.Tags()[c.Label])
		//}
	}
}

func AddBlockKeyCols(b Block, builder BlockBuilder) {
	cols := b.Cols()
	for _, c := range cols {
		if c.Key {
			builder.AddCol(c)
			//TODO: Set common value regardless of type
			//builder.SetCommonString(j, b.Tags()[c.Label])
		}
	}
}

// AddNewCols adds the columns of b onto builder that did not already exist.
// Returns the mapping of builder cols to block cols.
func AddNewCols(b Block, builder BlockBuilder) []int {
	cols := b.Cols()
	existing := builder.Cols()
	colMap := make([]int, len(existing))
	for j, c := range cols {
		found := false
		for ej, ec := range existing {
			if c.Label == ec.Label {
				colMap[ej] = j
				found = true
				break
			}
		}
		if !found {
			builder.AddCol(c)
			colMap = append(colMap, j)

			// TODO Set Common for any type
			//if c.IsTag() && c.Common {
			//	builder.SetCommonString(j, b.Tags()[c.Label])
			//}
		}
	}
	return colMap
}

// AppendBlock append data from block b onto builder.
// The colMap is a map of builder column index to block column index.
func AppendBlock(b Block, builder BlockBuilder, colMap []int) {
	if len(b.Cols()) == 0 {
		return
	}

	cols := builder.Cols()
	b.Do(func(cr ColReader) error {
		for j, c := range cols {
			nj := colMap[j]
			switch c.Type {
			case TBool:
				builder.AppendBools(j, cr.Bools(nj))
			case TInt:
				builder.AppendInts(j, cr.Ints(nj))
			case TUInt:
				builder.AppendUInts(j, cr.UInts(nj))
			case TFloat:
				builder.AppendFloats(j, cr.Floats(nj))
			case TString:
				builder.AppendStrings(j, cr.Strings(nj))
			case TTime:
				builder.AppendTimes(j, cr.Times(nj))
			default:
				PanicUnknownType(c.Type)
			}
		}
		return nil
	})
}

func AppendRecord(i int, cr ColReader, builder BlockBuilder) {
	for j, c := range builder.Cols() {
		if c.Common {
			continue
		}
		switch c.Type {
		case TBool:
			builder.AppendBool(j, cr.Bools(j)[i])
		case TInt:
			builder.AppendInt(j, cr.Ints(j)[i])
		case TUInt:
			builder.AppendUInt(j, cr.UInts(j)[i])
		case TFloat:
			builder.AppendFloat(j, cr.Floats(j)[i])
		case TString:
			builder.AppendString(j, cr.Strings(j)[i])
		case TTime:
			builder.AppendTime(j, cr.Times(j)[i])
		default:
			PanicUnknownType(c.Type)
		}
	}
}

// AddTags add columns to the builder for the given tags.
// It is assumed that all tags are common to all rows of this block.
func AddTags(t Tags, b BlockBuilder) {
	keys := t.Keys()
	for _, k := range keys {
		j := b.AddCol(ColMeta{
			Label: k,
			Type:  TString,
			//Kind:   TagColKind,
			Common: true,
		})
		b.SetCommonString(j, t[k])
	}
}

var NoDefaultValueColumn = fmt.Errorf("no default value column %q found.", DefaultValueColLabel)

func ValueCol(cols []ColMeta) (ColMeta, error) {
	for _, c := range cols {
		if c.Label == DefaultValueColLabel {
			return c, nil
		}
	}
	return ColMeta{}, NoDefaultValueColumn
}
func ValueIdx(cols []ColMeta) int {
	return ColIdx(DefaultValueColLabel, cols)
}
func ColIdx(label string, cols []ColMeta) int {
	for j, c := range cols {
		if c.Label == label {
			return j
		}
	}
	return -1
}

// BlockBuilder builds blocks that can be used multiple times
type BlockBuilder interface {
	SetBounds(Bounds)

	Key() PartitionKey

	NRows() int
	NCols() int
	Cols() []ColMeta

	// AddCol increases the size of the block by one column.
	// The index of the column is returned.
	AddCol(ColMeta) int

	// Set sets the value at the specified coordinates
	// The rows and columns must exist before calling set, otherwise Set panics.
	SetBool(i, j int, value bool)
	SetInt(i, j int, value int64)
	SetUInt(i, j int, value uint64)
	SetFloat(i, j int, value float64)
	SetString(i, j int, value string)
	SetTime(i, j int, value Time)

	// SetCommonString sets a single value for the entire column.
	SetCommonString(j int, value string)

	AppendBool(j int, value bool)
	AppendInt(j int, value int64)
	AppendUInt(j int, value uint64)
	AppendFloat(j int, value float64)
	AppendString(j int, value string)
	AppendTime(j int, value Time)

	AppendBools(j int, values []bool)
	AppendInts(j int, values []int64)
	AppendUInts(j int, values []uint64)
	AppendFloats(j int, values []float64)
	AppendStrings(j int, values []string)
	AppendTimes(j int, values []Time)

	// Sort the rows of the by the values of the columns in the order listed.
	Sort(cols []string, desc bool)

	// Clear removes all rows, while preserving the column meta data.
	ClearData()

	// Block returns the block that has been built.
	// Further modifications of the builder will not effect the returned block.
	Block() (Block, error)
}

type DataType int

const (
	TInvalid DataType = iota
	TBool
	TInt
	TUInt
	TFloat
	TString
	TTime
)

func (t DataType) String() string {
	switch t {
	case TInvalid:
		return "invalid"
	case TBool:
		return "bool"
	case TInt:
		return "int"
	case TUInt:
		return "uint"
	case TFloat:
		return "float"
	case TString:
		return "string"
	case TTime:
		return "time"
	default:
		return "unknown"
	}
}

type ColMeta struct {
	Label  string
	Type   DataType
	Key    bool // Key indicates that the column is part of the partition key
	Common bool // Common indicates that the value for the column is shared by all rows.
}

type BlockIterator interface {
	Do(f func(Block) error) error
}

// ColReader allows access to reading slices of column data.
// All data the ColReader exposes is guaranteed to be in memory.
// Once a ColReader goes out of scope all slices are considered invalid.
type ColReader interface {
	// Cols returns a list of column metadata.
	Cols() []ColMeta
	// Len returns the length of the slices.
	// All slices will have the same length.
	Len() int
	Bools(j int) []bool
	Ints(j int) []int64
	UInts(j int) []uint64
	Floats(j int) []float64
	Strings(j int) []string
	Times(j int) []Time
}

type Tags map[string]string

func (t Tags) Copy() Tags {
	nt := make(Tags, len(t))
	for k, v := range t {
		nt[k] = v
	}
	return nt
}

func (t Tags) Equal(o Tags) bool {
	if len(t) != len(o) {
		return false
	}
	for k, v := range t {
		if o[k] != v {
			return false
		}
	}
	return true
}

func (t Tags) Keys() []string {
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type TagsKey string

func (t Tags) Key() TagsKey {
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return TagsToKey(keys, t)
}

// Subset creates a new Tags that is a subset of t, using the list of keys.
// If a keys is provided that does not exist on t, then a subset is not possible and
// the boolean return value is false.
func (t Tags) Subset(keys []string) (Tags, bool) {
	subset := make(Tags, len(keys))
	for _, k := range keys {
		v, ok := t[k]
		if !ok {
			return nil, false
		}
		subset[k] = v
	}
	return subset, true
}

func (t Tags) IntersectingSubset(keys []string) Tags {
	subset := make(Tags, len(keys))
	for _, k := range keys {
		v, ok := t[k]
		if ok {
			subset[k] = v
		}
	}
	return subset
}

func TagsToKey(order []string, t Tags) TagsKey {
	var buf bytes.Buffer
	for i, k := range order {
		if i > 0 {
			buf.WriteRune(',')
		}
		buf.WriteString(k)
		buf.WriteRune('=')
		buf.WriteString(t[k])
	}
	return TagsKey(buf.String())
}

type blockMetadata struct {
	tags   Tags
	bounds Bounds
}

func (m blockMetadata) Tags() Tags {
	return m.tags
}
func (m blockMetadata) Bounds() Bounds {
	return m.bounds
}

type ColListBlockBuilder struct {
	blk   *ColListBlock
	alloc *Allocator
}

func NewColListBlockBuilder(a *Allocator) *ColListBlockBuilder {
	return &ColListBlockBuilder{
		blk:   new(ColListBlock),
		alloc: a,
	}
}

func (b ColListBlockBuilder) SetBounds(bounds Bounds) {
	b.blk.bounds = bounds
}
func (b ColListBlockBuilder) Bounds() Bounds {
	return b.blk.bounds
}

func (b ColListBlockBuilder) Key() PartitionKey {
	panic("not implemented")
}

func (b ColListBlockBuilder) NRows() int {
	return b.blk.nrows
}
func (b ColListBlockBuilder) NCols() int {
	return len(b.blk.cols)
}
func (b ColListBlockBuilder) Cols() []ColMeta {
	return b.blk.colMeta
}

func (b ColListBlockBuilder) AddCol(c ColMeta) int {
	var col column
	switch c.Type {
	case TBool:
		col = &boolColumn{
			ColMeta: c,
			alloc:   b.alloc,
		}
	case TInt:
		col = &intColumn{
			ColMeta: c,
			alloc:   b.alloc,
		}
	case TUInt:
		col = &uintColumn{
			ColMeta: c,
			alloc:   b.alloc,
		}
	case TFloat:
		col = &floatColumn{
			ColMeta: c,
			alloc:   b.alloc,
		}
	case TString:
		if c.Common {
			col = &commonStrColumn{
				ColMeta: c,
			}
		} else {
			col = &stringColumn{
				ColMeta: c,
				alloc:   b.alloc,
			}
		}
	case TTime:
		col = &timeColumn{
			ColMeta: c,
			alloc:   b.alloc,
		}
	default:
		PanicUnknownType(c.Type)
	}
	b.blk.colMeta = append(b.blk.colMeta, c)
	b.blk.cols = append(b.blk.cols, col)
	return len(b.blk.cols) - 1
}

func (b ColListBlockBuilder) SetBool(i int, j int, value bool) {
	b.checkColType(j, TBool)
	b.blk.cols[j].(*boolColumn).data[i] = value
}
func (b ColListBlockBuilder) AppendBool(j int, value bool) {
	b.checkColType(j, TBool)
	col := b.blk.cols[j].(*boolColumn)
	col.data = b.alloc.AppendBools(col.data, value)
	b.blk.nrows = len(col.data)
}
func (b ColListBlockBuilder) AppendBools(j int, values []bool) {
	b.checkColType(j, TBool)
	col := b.blk.cols[j].(*boolColumn)
	col.data = b.alloc.AppendBools(col.data, values...)
	b.blk.nrows = len(col.data)
}

func (b ColListBlockBuilder) SetInt(i int, j int, value int64) {
	b.checkColType(j, TInt)
	b.blk.cols[j].(*intColumn).data[i] = value
}
func (b ColListBlockBuilder) AppendInt(j int, value int64) {
	b.checkColType(j, TInt)
	col := b.blk.cols[j].(*intColumn)
	col.data = b.alloc.AppendInts(col.data, value)
	b.blk.nrows = len(col.data)
}
func (b ColListBlockBuilder) AppendInts(j int, values []int64) {
	b.checkColType(j, TInt)
	col := b.blk.cols[j].(*intColumn)
	col.data = b.alloc.AppendInts(col.data, values...)
	b.blk.nrows = len(col.data)
}

func (b ColListBlockBuilder) SetUInt(i int, j int, value uint64) {
	b.checkColType(j, TUInt)
	b.blk.cols[j].(*uintColumn).data[i] = value
}
func (b ColListBlockBuilder) AppendUInt(j int, value uint64) {
	b.checkColType(j, TUInt)
	col := b.blk.cols[j].(*uintColumn)
	col.data = b.alloc.AppendUInts(col.data, value)
	b.blk.nrows = len(col.data)
}
func (b ColListBlockBuilder) AppendUInts(j int, values []uint64) {
	b.checkColType(j, TUInt)
	col := b.blk.cols[j].(*uintColumn)
	col.data = b.alloc.AppendUInts(col.data, values...)
	b.blk.nrows = len(col.data)
}

func (b ColListBlockBuilder) SetFloat(i int, j int, value float64) {
	b.checkColType(j, TFloat)
	b.blk.cols[j].(*floatColumn).data[i] = value
}
func (b ColListBlockBuilder) AppendFloat(j int, value float64) {
	b.checkColType(j, TFloat)
	col := b.blk.cols[j].(*floatColumn)
	col.data = b.alloc.AppendFloats(col.data, value)
	b.blk.nrows = len(col.data)
}
func (b ColListBlockBuilder) AppendFloats(j int, values []float64) {
	b.checkColType(j, TFloat)
	col := b.blk.cols[j].(*floatColumn)
	col.data = b.alloc.AppendFloats(col.data, values...)
	b.blk.nrows = len(col.data)
}

func (b ColListBlockBuilder) SetString(i int, j int, value string) {
	b.checkColType(j, TString)
	b.blk.cols[j].(*stringColumn).data[i] = value
}
func (b ColListBlockBuilder) AppendString(j int, value string) {
	meta := b.blk.cols[j].Meta()
	CheckColType(meta, TString)
	if meta.Common {
		v := b.blk.cols[j].(*commonStrColumn).value
		if value != v {
			panic(fmt.Errorf("attempting to append a different value to the column %s, which has all common values", meta.Label))
		}
		return
	}
	col := b.blk.cols[j].(*stringColumn)
	col.data = b.alloc.AppendStrings(col.data, value)
	b.blk.nrows = len(col.data)
}
func (b ColListBlockBuilder) AppendStrings(j int, values []string) {
	b.checkColType(j, TString)
	col := b.blk.cols[j].(*stringColumn)
	col.data = b.alloc.AppendStrings(col.data, values...)
	b.blk.nrows = len(col.data)
}
func (b ColListBlockBuilder) SetCommonString(j int, value string) {
	meta := b.blk.cols[j].Meta()
	CheckColType(meta, TString)
	if !meta.Common {
		panic(fmt.Errorf("cannot set common value for column %s, column is not marked as common", meta.Label))
	}
	b.blk.cols[j].(*commonStrColumn).value = value
}

func (b ColListBlockBuilder) SetTime(i int, j int, value Time) {
	b.checkColType(j, TTime)
	b.blk.cols[j].(*timeColumn).data[i] = value
}
func (b ColListBlockBuilder) AppendTime(j int, value Time) {
	b.checkColType(j, TTime)
	col := b.blk.cols[j].(*timeColumn)
	col.data = b.alloc.AppendTimes(col.data, value)
	b.blk.nrows = len(col.data)
}
func (b ColListBlockBuilder) AppendTimes(j int, values []Time) {
	b.checkColType(j, TTime)
	col := b.blk.cols[j].(*timeColumn)
	col.data = b.alloc.AppendTimes(col.data, values...)
	b.blk.nrows = len(col.data)
}

func (b ColListBlockBuilder) checkColType(j int, typ DataType) {
	CheckColType(b.blk.colMeta[j], typ)
}

func CheckColType(col ColMeta, typ DataType) {
	if col.Type != typ {
		panic(fmt.Errorf("column %s is not of type %v", col.Label, typ))
	}
}

func PanicUnknownType(typ DataType) {
	panic(fmt.Errorf("unknown type %v", typ))
}

func (b ColListBlockBuilder) Block() (Block, error) {
	// Create copy in mutable state
	return b.blk.Copy(), nil
}

// RawBlock returns the underlying block being constructed.
// The Block returned will be modified by future calls to any BlockBuilder methods.
func (b ColListBlockBuilder) RawBlock() *ColListBlock {
	// Create copy in mutable state
	return b.blk
}

func (b ColListBlockBuilder) ClearData() {
	for _, c := range b.blk.cols {
		c.Clear()
	}
	b.blk.nrows = 0
}

func (b ColListBlockBuilder) Sort(cols []string, desc bool) {
	colIdxs := make([]int, len(cols))
	for i, label := range cols {
		for j, c := range b.blk.colMeta {
			if c.Label == label {
				colIdxs[i] = j
				break
			}
		}
	}
	s := colListBlockSorter{cols: colIdxs, desc: desc, b: b.blk}
	sort.Sort(s)
}

// ColListBlock implements Block using list of columns.
// All data for the block is stored in RAM.
// As a result At* methods are provided directly on the block for easy access.
type ColListBlock struct {
	bounds Bounds
	//tags   Tags

	colMeta []ColMeta
	cols    []column
	nrows   int

	refCount int32
}

func (b *ColListBlock) RefCount(n int) {
	c := atomic.AddInt32(&b.refCount, int32(n))
	if c == 0 {
		for _, c := range b.cols {
			c.Clear()
		}
	}
}

func (b *ColListBlock) Bounds() Bounds {
	return b.bounds
}

func (b *ColListBlock) Key() PartitionKey {
	panic("not implemented")
}
func (b *ColListBlock) Cols() []ColMeta {
	return b.colMeta
}
func (b *ColListBlock) NRows() int {
	return b.nrows
}

func (b *ColListBlock) Len() int {
	return b.nrows
}

func (b *ColListBlock) Do(f func(ColReader) error) error {
	return f(b)
}

func (b *ColListBlock) Bools(j int) []bool {
	CheckColType(b.colMeta[j], TBool)
	return b.cols[j].(*boolColumn).data
}
func (b *ColListBlock) Ints(j int) []int64 {
	CheckColType(b.colMeta[j], TInt)
	return b.cols[j].(*intColumn).data
}
func (b *ColListBlock) UInts(j int) []uint64 {
	CheckColType(b.colMeta[j], TUInt)
	return b.cols[j].(*uintColumn).data
}
func (b *ColListBlock) Floats(j int) []float64 {
	CheckColType(b.colMeta[j], TFloat)
	return b.cols[j].(*floatColumn).data
}
func (b *ColListBlock) Strings(j int) []string {
	meta := b.colMeta[j]
	CheckColType(meta, TString)
	return b.cols[j].(*stringColumn).data
}
func (b *ColListBlock) Times(j int) []Time {
	CheckColType(b.colMeta[j], TTime)
	return b.cols[j].(*timeColumn).data
}

func (b *ColListBlock) Copy() *ColListBlock {
	cpy := new(ColListBlock)
	cpy.bounds = b.bounds
	cpy.nrows = b.nrows

	cpy.colMeta = make([]ColMeta, len(b.colMeta))
	copy(cpy.colMeta, b.colMeta)

	cpy.cols = make([]column, len(b.cols))
	for i, c := range b.cols {
		cpy.cols[i] = c.Copy()
	}

	return cpy
}

type colListBlockSorter struct {
	cols []int
	desc bool
	b    *ColListBlock
}

func (c colListBlockSorter) Len() int {
	return c.b.nrows
}

func (c colListBlockSorter) Less(x int, y int) (less bool) {
	for _, j := range c.cols {
		if !c.b.cols[j].Equal(x, y) {
			less = c.b.cols[j].Less(x, y)
			break
		}
	}
	if c.desc {
		less = !less
	}
	return
}

func (c colListBlockSorter) Swap(x int, y int) {
	for _, col := range c.b.cols {
		col.Swap(x, y)
	}
}

type column interface {
	Meta() ColMeta
	Clear()
	Copy() column
	Equal(i, j int) bool
	Less(i, j int) bool
	Swap(i, j int)
}

type boolColumn struct {
	ColMeta
	data  []bool
	alloc *Allocator
}

func (c *boolColumn) Meta() ColMeta {
	return c.ColMeta
}

func (c *boolColumn) Clear() {
	c.alloc.Free(len(c.data), boolSize)
	c.data = c.data[0:0]
}
func (c *boolColumn) Copy() column {
	cpy := &boolColumn{
		ColMeta: c.ColMeta,
		alloc:   c.alloc,
	}
	l := len(c.data)
	cpy.data = c.alloc.Bools(l, l)
	copy(cpy.data, c.data)
	return cpy
}
func (c *boolColumn) Equal(i, j int) bool {
	return c.data[i] == c.data[j]
}
func (c *boolColumn) Less(i, j int) bool {
	if c.data[i] == c.data[j] {
		return false
	}
	return c.data[i]
}
func (c *boolColumn) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

type intColumn struct {
	ColMeta
	data  []int64
	alloc *Allocator
}

func (c *intColumn) Meta() ColMeta {
	return c.ColMeta
}

func (c *intColumn) Clear() {
	c.alloc.Free(len(c.data), int64Size)
	c.data = c.data[0:0]
}
func (c *intColumn) Copy() column {
	cpy := &intColumn{
		ColMeta: c.ColMeta,
		alloc:   c.alloc,
	}
	l := len(c.data)
	cpy.data = c.alloc.Ints(l, l)
	copy(cpy.data, c.data)
	return cpy
}
func (c *intColumn) Equal(i, j int) bool {
	return c.data[i] == c.data[j]
}
func (c *intColumn) Less(i, j int) bool {
	return c.data[i] < c.data[j]
}
func (c *intColumn) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

type uintColumn struct {
	ColMeta
	data  []uint64
	alloc *Allocator
}

func (c *uintColumn) Meta() ColMeta {
	return c.ColMeta
}

func (c *uintColumn) Clear() {
	c.alloc.Free(len(c.data), uint64Size)
	c.data = c.data[0:0]
}
func (c *uintColumn) Copy() column {
	cpy := &uintColumn{
		ColMeta: c.ColMeta,
		alloc:   c.alloc,
	}
	l := len(c.data)
	cpy.data = c.alloc.UInts(l, l)
	copy(cpy.data, c.data)
	return cpy
}
func (c *uintColumn) Equal(i, j int) bool {
	return c.data[i] == c.data[j]
}
func (c *uintColumn) Less(i, j int) bool {
	return c.data[i] < c.data[j]
}
func (c *uintColumn) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

type floatColumn struct {
	ColMeta
	data  []float64
	alloc *Allocator
}

func (c *floatColumn) Meta() ColMeta {
	return c.ColMeta
}

func (c *floatColumn) Clear() {
	c.alloc.Free(len(c.data), float64Size)
	c.data = c.data[0:0]
}
func (c *floatColumn) Copy() column {
	cpy := &floatColumn{
		ColMeta: c.ColMeta,
		alloc:   c.alloc,
	}
	l := len(c.data)
	cpy.data = c.alloc.Floats(l, l)
	copy(cpy.data, c.data)
	return cpy
}
func (c *floatColumn) Equal(i, j int) bool {
	return c.data[i] == c.data[j]
}
func (c *floatColumn) Less(i, j int) bool {
	return c.data[i] < c.data[j]
}
func (c *floatColumn) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

type stringColumn struct {
	ColMeta
	data  []string
	alloc *Allocator
}

func (c *stringColumn) Meta() ColMeta {
	return c.ColMeta
}

func (c *stringColumn) Clear() {
	c.alloc.Free(len(c.data), stringSize)
	c.data = c.data[0:0]
}
func (c *stringColumn) Copy() column {
	cpy := &stringColumn{
		ColMeta: c.ColMeta,
		alloc:   c.alloc,
	}

	l := len(c.data)
	cpy.data = c.alloc.Strings(l, l)
	copy(cpy.data, c.data)
	return cpy
}
func (c *stringColumn) Equal(i, j int) bool {
	return c.data[i] == c.data[j]
}
func (c *stringColumn) Less(i, j int) bool {
	return c.data[i] < c.data[j]
}
func (c *stringColumn) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

type timeColumn struct {
	ColMeta
	data  []Time
	alloc *Allocator
}

func (c *timeColumn) Meta() ColMeta {
	return c.ColMeta
}

func (c *timeColumn) Clear() {
	c.alloc.Free(len(c.data), timeSize)
	c.data = c.data[0:0]
}
func (c *timeColumn) Copy() column {
	cpy := &timeColumn{
		ColMeta: c.ColMeta,
		alloc:   c.alloc,
	}
	l := len(c.data)
	cpy.data = c.alloc.Times(l, l)
	copy(cpy.data, c.data)
	return cpy
}
func (c *timeColumn) Equal(i, j int) bool {
	return c.data[i] == c.data[j]
}
func (c *timeColumn) Less(i, j int) bool {
	return c.data[i] < c.data[j]
}
func (c *timeColumn) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

//commonStrColumn has the same string value for all rows
type commonStrColumn struct {
	ColMeta
	value string
}

func (c *commonStrColumn) Meta() ColMeta {
	return c.ColMeta
}
func (c *commonStrColumn) Clear() {
}
func (c *commonStrColumn) Copy() column {
	cpy := new(commonStrColumn)
	*cpy = *c
	return cpy
}
func (c *commonStrColumn) Equal(i, j int) bool {
	return true
}
func (c *commonStrColumn) Less(i, j int) bool {
	return false
}
func (c *commonStrColumn) Swap(i, j int) {}

type BlockBuilderCache interface {
	// BlockBuilder returns an existing or new BlockBuilder for the given meta data.
	// The boolean return value indicates if BlockBuilder is new.
	BlockBuilder(key PartitionKey) (BlockBuilder, bool)
	ForEachBuilder(f func(PartitionKey, BlockBuilder))
}

type blockBuilderCache struct {
	blocks map[uint64][]blockState
	alloc  *Allocator

	triggerSpec query.TriggerSpec
}

func NewBlockBuilderCache(a *Allocator) *blockBuilderCache {
	return &blockBuilderCache{
		blocks: make(map[uint64][]blockState),
		alloc:  a,
	}
}

type blockState struct {
	key     PartitionKey
	builder BlockBuilder
	trigger Trigger
}

func (d *blockBuilderCache) SetTriggerSpec(ts query.TriggerSpec) {
	d.triggerSpec = ts
}

func (d *blockBuilderCache) Block(key PartitionKey) (Block, error) {
	b, ok := d.lookupState(key)
	if !ok {
		return nil, errors.New("block not found")
	}
	return b.builder.Block()
}

func computeKeyHash(key PartitionKey) uint64 {
	h := fnv.New64()
	for j, c := range key.Cols() {
		h.Write([]byte(c.Label))
		switch c.Type {
		case TBool:
			if key.ValueBool(j) {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		case TInt:
			binary.Write(h, binary.BigEndian, key.ValueInt(j))
		case TUInt:
			binary.Write(h, binary.BigEndian, key.ValueUInt(j))
		case TFloat:
			binary.Write(h, binary.BigEndian, math.Float64bits(key.ValueFloat(j)))
		case TString:
			h.Write([]byte(key.ValueString(j)))
		case TTime:
			binary.Write(h, binary.BigEndian, uint64(key.ValueTime(j)))
		}
	}
	return h.Sum64()
}

// BlockBuilder will return the builder for the specified block.
// If no builder exists, one will be created.
func (d *blockBuilderCache) BlockBuilder(key PartitionKey) (BlockBuilder, bool) {
	b, ok := d.lookupState(key)
	if !ok {
		builder := NewColListBlockBuilder(d.alloc)
		t := NewTriggerFromSpec(d.triggerSpec)
		b = blockState{
			key:     key,
			builder: builder,
			trigger: t,
		}
		d.blocks[key.Hash()] = append(d.blocks[key.Hash()], b)
	}
	return b.builder, !ok
}

func (d *blockBuilderCache) lookupState(key PartitionKey) (blockState, bool) {
	h := key.Hash()
	bs, ok := d.blocks[h]
	if !ok {
		return blockState{}, false
	}
	if len(bs) == 1 {
		return bs[0], false
	}
	for _, b := range bs {
		if PartitionKeyCompare(b.key, key) {
			return b, false
		}
	}
	return blockState{}, false
}

func (d *blockBuilderCache) ForEachBuilder(f func(PartitionKey, BlockBuilder)) {
	for _, bs := range d.blocks {
		for _, b := range bs {
			f(b.key, b.builder)
		}
	}
}

func (d *blockBuilderCache) DiscardBlock(key PartitionKey) {
	b, ok := d.lookupState(key)
	if ok {
		b.builder.ClearData()
	}
}
func (d *blockBuilderCache) ExpireBlock(key PartitionKey) {
	b, ok := d.lookupState(key)
	if ok {
		b.builder.ClearData()
		panic("not implemented")
		//delete(d.blocks, key)
	}
}

func (d *blockBuilderCache) ForEach(f func(PartitionKey)) {
	for _, bs := range d.blocks {
		for _, b := range bs {
			f(b.key)
		}
	}
}

func (d *blockBuilderCache) ForEachWithContext(f func(PartitionKey, Trigger, BlockContext)) {
	for _, bs := range d.blocks {
		for _, b := range bs {
			f(b.key, b.trigger, BlockContext{
				Key:   b.key,
				Count: b.builder.NRows(),
			})
		}
	}
}
