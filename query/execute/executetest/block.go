package executetest

import (
	"fmt"

	"github.com/influxdata/ifql/query/execute"
)

type Block struct {
	PartitionKey execute.PartitionKey
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
		cols := make([]execute.ColMeta, 0, len(b.ColMeta))
		values := make([]interface{}, 0, len(b.ColMeta))
		for j, c := range b.ColMeta {
			if c.Key {
				cols = append(cols, c)
				values = append(values, b.Data[0][j])
			}
		}
		b.PartitionKey = execute.NewPartitionKey(cols, values)
	}
	return b.PartitionKey
}

func (b *Block) Do(f func(execute.ColReader) error) error {
	for _, r := range b.Data {
		if err := f(ColReader{
			cols: b.ColMeta,
			row:  r,
		}); err != nil {
			return err
		}
	}
	return nil
}

type ColReader struct {
	cols []execute.ColMeta
	row  []interface{}
}

func (cr ColReader) Cols() []execute.ColMeta {
	return cr.cols
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

func BlocksFromCache(c execute.DataCache) []*Block {
	var blocks []*Block
	c.ForEach(func(key execute.PartitionKey) {
		b, err := c.Block(key)
		if err != nil {
			panic(err)
		}
		blocks = append(blocks, ConvertBlock(b))
	})
	return blocks
}

func ConvertBlock(b execute.Block) *Block {
	blk := &Block{
		ColMeta: b.Cols(),
	}

	b.Do(func(cr execute.ColReader) error {
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
	return blk
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
