package execute

import (
	"io"
	"sort"
	"strconv"
	"strings"
)

const fixedWidthTimeFmt = "2006-01-02T15:04:05.000000000Z"

// Formatter writes a block to a Writer.
type Formatter struct {
	b         Block
	widths    []int
	maxWidth  int
	newWidths []int
	pad       []byte
	dash      []byte
	// fmtBuf is used to format values
	fmtBuf [64]byte

	opts FormatOptions

	cols orderedCols
}
type FormatOptions struct {
	// RepeatHeaderCount is the number of rows to print before printing the header again.
	// If zero then the headers are not repeated.
	RepeatHeaderCount int
}

func DefaultFormatOptions() *FormatOptions {
	return &FormatOptions{}
}

var eol = []byte{'\n'}

// NewFormatter creates a Formatter for a given block.
// If opts is nil, the DefaultFormatOptions are used.
func NewFormatter(b Block, opts *FormatOptions) *Formatter {
	if opts == nil {
		opts = DefaultFormatOptions()
	}
	return &Formatter{
		b:    b,
		opts: *opts,
	}
}

type writeToHelper struct {
	w   io.Writer
	n   int64
	err error
}

func (w *writeToHelper) write(data []byte) {
	if w.err != nil {
		return
	}
	n, err := w.w.Write(data)
	w.n += int64(n)
	w.err = err
}

var minWidthsByType = map[DataType]int{
	TBool:    12,
	TInt:     26,
	TUInt:    27,
	TFloat:   28,
	TString:  22,
	TTime:    len(fixedWidthTimeFmt),
	TInvalid: 10,
}

// WriteTo writes the formatted block data to w.
func (f *Formatter) WriteTo(out io.Writer) (int64, error) {
	w := &writeToHelper{w: out}

	// Sort cols
	cols := f.b.Cols()
	f.cols = newOrderedCols(cols)
	sort.Sort(f.cols)

	// Compute header widths
	f.widths = make([]int, len(cols))
	for j, c := range cols {
		l := len(c.Label)
		min := minWidthsByType[c.Type]
		if min > l {
			l = min
		}
		if l > f.widths[j] {
			f.widths[j] = l
		}
		if l > f.maxWidth {
			f.maxWidth = l
		}
	}

	// Write Block header
	w.write([]byte("Block: keys: ["))
	labels := make([]string, len(f.b.Key().Cols()))
	for i, c := range f.b.Key().Cols() {
		labels[i] = c.Label
	}
	w.write([]byte(strings.Join(labels, ", ")))
	w.write([]byte("]"))
	w.write(eol)

	// Check err and return early
	if w.err != nil {
		return w.n, w.err
	}

	// Write rows
	r := 0
	f.b.Do(func(cr ColReader) error {
		if r == 0 {
			l := cr.Len()
			for i := 0; i < l; i++ {
				for oj, c := range f.cols.cols {
					j := f.cols.Idx(oj)
					buf := f.valueBuf(i, j, c.Type, cr)
					l := len(buf)
					if l > f.widths[j] {
						f.widths[j] = l
					}
					if l > f.maxWidth {
						f.maxWidth = l
					}
				}
			}
			f.makePaddingBuffers()
			f.writeHeader(w)
			f.writeHeaderSeparator(w)
			f.newWidths = make([]int, len(f.widths))
			copy(f.newWidths, f.widths)
		}
		l := cr.Len()
		for i := 0; i < l; i++ {
			for oj, c := range f.cols.cols {
				j := f.cols.Idx(oj)
				buf := f.valueBuf(i, j, c.Type, cr)
				l := len(buf)
				padding := f.widths[j] - l
				if padding >= 0 {
					w.write(f.pad[:padding])
					w.write(buf)
				} else {
					//TODO make unicode friendly
					w.write(buf[:f.widths[j]-3])
					w.write([]byte{'.', '.', '.'})
				}
				w.write(f.pad[:2])
				if l > f.newWidths[j] {
					f.newWidths[j] = l
				}
				if l > f.maxWidth {
					f.maxWidth = l
				}
			}
			w.write(eol)
			r++
			if f.opts.RepeatHeaderCount > 0 && r%f.opts.RepeatHeaderCount == 0 {
				copy(f.widths, f.newWidths)
				f.makePaddingBuffers()
				f.writeHeaderSeparator(w)
				f.writeHeader(w)
				f.writeHeaderSeparator(w)
			}
		}
		return w.err
	})
	return w.n, w.err
}

func (f *Formatter) makePaddingBuffers() {
	if len(f.pad) != f.maxWidth {
		f.pad = make([]byte, f.maxWidth)
		for i := range f.pad {
			f.pad[i] = ' '
		}
	}
	if len(f.dash) != f.maxWidth {
		f.dash = make([]byte, f.maxWidth)
		for i := range f.dash {
			f.dash[i] = '-'
		}
	}
}

func (f *Formatter) writeHeader(w *writeToHelper) {
	for oj, c := range f.cols.cols {
		j := f.cols.Idx(oj)
		buf := append(append([]byte(c.Label), ':'), []byte(c.Type.String())...)
		w.write(f.pad[:f.widths[j]-len(buf)])
		w.write(buf)
		w.write(f.pad[:2])
	}
	w.write(eol)
}

func (f *Formatter) writeHeaderSeparator(w *writeToHelper) {
	for oj := range f.cols.cols {
		j := f.cols.Idx(oj)
		w.write(f.dash[:f.widths[j]])
		w.write(f.pad[:2])
	}
	w.write(eol)
}

func (f *Formatter) valueBuf(i, j int, typ DataType, cr ColReader) (buf []byte) {
	switch typ {
	case TBool:
		buf = strconv.AppendBool(f.fmtBuf[0:0], cr.Bools(j)[i])
	case TInt:
		buf = strconv.AppendInt(f.fmtBuf[0:0], cr.Ints(j)[i], 10)
	case TUInt:
		buf = strconv.AppendUint(f.fmtBuf[0:0], cr.UInts(j)[i], 10)
	case TFloat:
		// TODO allow specifying format and precision
		buf = strconv.AppendFloat(f.fmtBuf[0:0], cr.Floats(j)[i], 'f', -1, 64)
	case TString:
		buf = []byte(cr.Strings(j)[i])
	case TTime:
		buf = []byte(cr.Times(j)[i].String())
	}
	return
}

// orderedCols sorts a list of columns:
//
// * time
// * common tags sorted by label
// * other tags sorted by label
// * value
//
type orderedCols struct {
	indexMap []int
	cols     []ColMeta
}

func newOrderedCols(cols []ColMeta) orderedCols {
	indexMap := make([]int, len(cols))
	for i := range indexMap {
		indexMap[i] = i
	}
	cpy := make([]ColMeta, len(cols))
	copy(cpy, cols)
	return orderedCols{
		indexMap: indexMap,
		cols:     cpy,
	}
}

func (o orderedCols) Idx(oj int) int {
	return o.indexMap[oj]
}

func (o orderedCols) Len() int { return len(o.cols) }
func (o orderedCols) Swap(i int, j int) {
	o.cols[i], o.cols[j] = o.cols[j], o.cols[i]
	o.indexMap[i], o.indexMap[j] = o.indexMap[j], o.indexMap[i]
}

func (o orderedCols) Less(i int, j int) bool {
	// Time column is always first
	if o.cols[i].Label == DefaultTimeColLabel {
		return true
	}
	if o.cols[j].Label == DefaultTimeColLabel {
		return false
	}

	// Common cols before other cols
	if o.cols[i].Common && !o.cols[j].Common {
		return true
	}
	if !o.cols[i].Common && o.cols[j].Common {
		return false
	}

	// Value column is always last
	if o.cols[i].Label == DefaultValueColLabel {
		return false
	}
	if o.cols[j].Label == DefaultValueColLabel {
		return true
	}

	// within a class sort by label
	return o.cols[i].Label < o.cols[j].Label
}
