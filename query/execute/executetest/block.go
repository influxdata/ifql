package executetest

import (
	"fmt"

	"github.com/influxdata/ifql/query/execute"
)

type Block struct {
	PartitionKey execute.PartitionKey
	KeyCols      []string
	ColMeta      []execute.ColMeta
	// Data is a list of rows, i.e. Data[row][col]
	// Each row must be a list with length equal to len(ColMeta)
	Data [][]interface{}
}

func (b *Block) RefCount(n int) {}

func (b *Block) Cols() []execute.ColMeta {
	return b.ColMeta
}

func (b *Block) Key() execute.PartitionKey {
	if b.PartitionKey == nil {
		cols := make([]execute.ColMeta, len(b.KeyCols))
		values := make([]interface{}, len(b.KeyCols))
		for j, label := range b.KeyCols {
			idx := execute.ColIdx(label, b.ColMeta)
			cols[j] = b.ColMeta[idx]
			values[j] = b.Data[0][idx]
		}
		b.PartitionKey = execute.NewPartitionKey(cols, values)
	}
	return b.PartitionKey
}

func (b *Block) Do(f func(execute.ColReader) error) error {
	for _, r := range b.Data {
		if err := f(ColReader{
			key:  b.Key(),
			cols: b.ColMeta,
			row:  r,
		}); err != nil {
			return err
		}
	}
	return nil
}

type ColReader struct {
	key  execute.PartitionKey
	cols []execute.ColMeta
	row  []interface{}
}

func (cr ColReader) Cols() []execute.ColMeta {
	return cr.cols
}

func (cr ColReader) Key() execute.PartitionKey {
	return cr.key
}
func (cr ColReader) Len() int {
	return 1
}

func (cr ColReader) Bools(j int) []bool {
	return []bool{cr.row[j].(bool)}
}

func (cr ColReader) Ints(j int) []int64 {
	return []int64{cr.row[j].(int64)}
}

func (cr ColReader) UInts(j int) []uint64 {
	return []uint64{cr.row[j].(uint64)}
}

func (cr ColReader) Floats(j int) []float64 {
	return []float64{cr.row[j].(float64)}
}

func (cr ColReader) Strings(j int) []string {
	return []string{cr.row[j].(string)}
}

func (cr ColReader) Times(j int) []execute.Time {
	return []execute.Time{cr.row[j].(execute.Time)}
}

func BlocksFromCache(c execute.DataCache) (blocks []*Block, err error) {
	c.ForEach(func(key execute.PartitionKey) {
		if err != nil {
			return
		}
		var b execute.Block
		b, err = c.Block(key)
		if err != nil {
			return
		}
		var cb *Block
		cb, err = ConvertBlock(b)
		if err != nil {
			return
		}
		blocks = append(blocks, cb)
	})
	return blocks, nil
}

func ConvertBlock(b execute.Block) (*Block, error) {
	blk := &Block{
		ColMeta: b.Cols(),
	}

	keyCols := b.Key().Cols()
	if len(keyCols) > 0 {
		keys := make([]string, len(keyCols))
		for j, c := range keyCols {
			keys[j] = c.Label
		}
		blk.KeyCols = keys
	}

	err := b.Do(func(cr execute.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			row := make([]interface{}, len(blk.ColMeta))
			for j, c := range blk.ColMeta {
				var v interface{}
				switch c.Type {
				case execute.TBool:
					v = cr.Bools(j)[i]
				case execute.TInt:
					v = cr.Ints(j)[i]
				case execute.TUInt:
					v = cr.UInts(j)[i]
				case execute.TFloat:
					v = cr.Floats(j)[i]
				case execute.TString:
					v = cr.Strings(j)[i]
				case execute.TTime:
					v = cr.Times(j)[i]
				default:
					panic(fmt.Errorf("unknown column type %s", c.Type))
				}
				row[j] = v
			}
			blk.Data = append(blk.Data, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return blk, nil
}

type SortedBlocks []*Block

func (b SortedBlocks) Len() int {
	return len(b)
}

func (b SortedBlocks) Less(i int, j int) bool {
	return b[i].Key().Less(b[j].Key())
}

func (b SortedBlocks) Swap(i int, j int) {
	b[i], b[j] = b[j], b[i]
}
