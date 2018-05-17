package functions

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/line-protocol"
	"github.com/pkg/errors"
)

const (
	OutputHTTPKind           = "outputHTTP"
	defaultOutputHTTPTimeout = 1 * time.Second
)

var outputHTTPUserAgent = "ifqld/dev"

func newOutPutClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConnsPerHost:   runtime.GOMAXPROCS(0) + 1,
		},
	}
}

var outputHTTPKeepAliveClient = newOutPutClient()

// this is used so we can get better validation on marshaling, innerOutputHTTPOpSpec and OutputHTTPOpSpec
// need to have identical fields
type innerOutputHTTPOpSpec OutputHTTPOpSpec

type OutputHTTPOpSpec struct {
	Addr         string            `json:"addr"`
	Method       string            `json:"method"` // default behavior should be POST
	Name         string            `json:"name"`
	Headers      map[string]string `json:"headers"`   // TODO: implement Headers after bug with keys and arrays and objects is fixed (new parser implemented, with string literals as keys)
	URLParams    map[string]string `json:"urlparams"` // TODO: implement URLParams after bug with keys and arrays and objects is fixed (new parser implemented, with string literals as keys)
	Timeout      time.Duration     `json:"timeout"`   // default to something reasonable if zero
	NoKeepAlive  bool              `json:"nokeepalive"`
	TimeColumn   string            `json:"time_column"`
	TagColumns   []string          `json:"tag_columns"`
	ValueColumns []string          `json:"value_columns"`
}

func init() {
	query.RegisterFunction(OutputHTTPKind, createOutputHTTPOpSpec, outputHTTPSignature)
	query.RegisterOpSpec(OutputHTTPKind,
		func() query.OperationSpec { return &OutputHTTPOpSpec{} })
	plan.RegisterProcedureSpec(OutputHTTPKind, newOutputHTTPProcedure, OutputHTTPKind)
	execute.RegisterTransformation(OutputHTTPKind, createOutputHTTPTransformation)
}

// ReadArgs loads a query.Arguments into OutputHTTPOpSpec.  It sets several default values.
// If the http method isn't set, it defaults to POST, it also uppercases the http method.
// If the time_column isn't set, it defaults to execute.TimeColLabel.
// If the value_column isn't set it defaults to a []string{execute.DefaultValueColLabel}.
func (o *OutputHTTPOpSpec) ReadArgs(args query.Arguments) error {
	var err error
	o.Addr, err = args.GetRequiredString("addr")
	if err != nil {
		return err
	}

	o.Name, err = args.GetRequiredString("name")
	if err != nil {
		return err
	}

	var ok bool
	o.Method, ok, err = args.GetString("method")
	if err != nil {
		return err
	}
	if !ok {
		o.Method = "POST"
	}
	o.Method = strings.ToUpper(o.Method)

	timeout, ok, err := args.GetDuration("timeout")
	if err != nil {
		return err
	}
	if !ok {
		o.Timeout = defaultOutputHTTPTimeout
	} else {
		o.Timeout = time.Duration(timeout)
	}

	o.TimeColumn, ok, err = args.GetString("time_column")
	if err != nil {
		return err
	}
	if !ok {
		o.TimeColumn = execute.TimeColLabel
	}

	tagColumns, ok, err := args.GetArray("tag_columns", semantic.String)
	if err != nil {
		return err
	}
	o.TagColumns = o.TagColumns[:0]
	if ok {
		for i := 0; i < tagColumns.Len(); i++ {
			o.TagColumns = append(o.TagColumns, tagColumns.Get(i).Str())
		}
		sort.Strings(o.TagColumns)
	}

	valueColumns, ok, err := args.GetArray("value_columns", semantic.String)
	if err != nil {
		return err
	}
	o.ValueColumns = o.ValueColumns[:0]

	if !ok || valueColumns.Len() == 0 {
		o.ValueColumns = append(o.ValueColumns, execute.DefaultValueColLabel)
	} else {
		for i := 0; i < valueColumns.Len(); i++ {
			o.TagColumns = append(o.ValueColumns, valueColumns.Get(i).Str())
		}
		sort.Strings(o.TagColumns)
	}

	// TODO: get other headers working!
	o.Headers = map[string]string{"Content-Type": "application/vnd.influx"}

	return err

}

func createOutputHTTPOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
	if err := a.AddParentFromArgs(args); err != nil {
		return nil, err
	}
	s := new(OutputHTTPOpSpec)
	if err := s.ReadArgs(args); err != nil {
		return nil, err
	}
	// if err := s.AggregateConfig.ReadArgs(args); err != nil {
	// 	return s, err
	// }
	return s, nil
}

// UnmarshalJSON unmarshals and validates OutputHTTPOpSpec into JSON.
func (o *OutputHTTPOpSpec) UnmarshalJSON(b []byte) (err error) {

	if err = json.Unmarshal(b, (*innerOutputHTTPOpSpec)(o)); err != nil {
		return err
	}
	u, err := url.ParseRequestURI(o.Addr)
	if err != nil {
		return err
	}
	if !(u.Scheme == "https" || u.Scheme == "http" || u.Scheme == "") {
		return fmt.Errorf("Scheme must be http or https but was %s", u.Scheme)
	}
	return nil
}

var outputHTTPSignature = query.DefaultFunctionSignature()

func (OutputHTTPOpSpec) Kind() query.OperationKind {
	return OutputHTTPKind
}

type OutputHTTPProcedureSpec struct {
	Spec *OutputHTTPOpSpec
}

func (o *OutputHTTPProcedureSpec) Kind() plan.ProcedureKind {
	return CountKind
}

func (o *OutputHTTPProcedureSpec) Copy() plan.ProcedureSpec {
	return &OutputHTTPProcedureSpec{}
}

func newOutputHTTPProcedure(qs query.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	spec, ok := qs.(*OutputHTTPOpSpec)
	if !ok && spec != nil {
		return nil, fmt.Errorf("invalid spec type %T", qs)
	}
	return &OutputHTTPProcedureSpec{Spec: spec}, nil
}

func createOutputHTTPTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
	s, ok := spec.(*OutputHTTPProcedureSpec)
	if !ok {
		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
	}
	cache := execute.NewBlockBuilderCache(a.Allocator())
	d := execute.NewDataset(id, mode, cache)
	t := NewOutputHTTPTransformation(d, cache, s)
	return t, d, nil
}

type OutputHTTPTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache
	spec  *OutputHTTPProcedureSpec
}

func (t *OutputHTTPTransformation) RetractBlock(id execute.DatasetID, meta execute.BlockMetadata) error {
	key := execute.ToBlockKey(meta)
	return t.d.RetractBlock(key)
}

func NewOutputHTTPTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *OutputHTTPProcedureSpec) *OutputHTTPTransformation {

	return &OutputHTTPTransformation{
		d:     d,
		cache: cache,
		spec:  spec,
	}
}

type httpOutputMetric struct {
	tags   []*protocol.Tag
	fields []*protocol.Field
	name   string
	t      time.Time
}

func (m *httpOutputMetric) TagList() []*protocol.Tag {
	return m.tags
}
func (m *httpOutputMetric) FieldList() []*protocol.Field {
	return m.fields
}

func (m *httpOutputMetric) truncateTagsAndFields() {
	m.fields = m.fields[:0]
	m.tags = m.tags[:0]

}

func (m *httpOutputMetric) Name() string {
	return m.name
}

func (m *httpOutputMetric) Time() time.Time {
	return m.t
}

type idxType struct {
	Idx  int
	Type execute.DataType
}

func (t *OutputHTTPTransformation) Process(id execute.DatasetID, b execute.Block) error {
	pr, pw := io.Pipe() // TODO: replce the pipe with something faster
	m := &httpOutputMetric{}
	e := protocol.NewEncoder(pw)
	e.FailOnFieldErr(true)
	e.SetFieldSortOrder(protocol.SortFields)
	cols := b.Cols()
	labels := make(map[string]idxType, len(cols))
	for i, col := range cols {
		labels[col.Label] = idxType{Idx: i, Type: col.Type}
	}
	// do time
	timeColLabel := t.spec.Spec.TimeColumn
	timeColIdx, ok := labels[timeColLabel]
	if !ok {
		return errors.New("Could not get time column")
	}
	if timeColIdx.Type != execute.TTime {
		return fmt.Errorf("column %s is not of type %s", timeColLabel, timeColIdx.Type)
	}
	iter := b.Col(timeColIdx.Idx)
	var err error
	go func() {
		m.name = t.spec.Spec.Name
		iter.DoTime(func(et []execute.Time, er execute.RowReader) {
			if err != nil {
				return
			}
			m.truncateTagsAndFields()
			for i, col := range er.Cols() {
				col := col
				switch {
				case col.Label == timeColLabel:
					m.t = er.AtTime(0, i).Time()
				case sort.SearchStrings(t.spec.Spec.ValueColumns, col.Label) < len(t.spec.Spec.ValueColumns): // do thing to get values
					switch col.Type {
					case execute.TFloat:
						m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.AtFloat(0, i)})
					case execute.TInt:
						m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.AtInt(0, i)})
					case execute.TUInt:
						m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.AtUInt(0, i)})
					case execute.TString:
						m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.AtString(0, i)})
					case execute.TTime:
						m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.AtTime(0, i)})
					case execute.TBool:
						m.fields = append(m.fields, &protocol.Field{Key: col.Label, Value: er.AtBool(0, i)})
					default:
						err = errors.New("invalid type")
					}
				case sort.SearchStrings(t.spec.Spec.TagColumns, col.Label) < len(t.spec.Spec.TagColumns): // do thing to get tag
					m.tags = append(m.tags, &protocol.Tag{Key: col.Label, Value: er.AtString(0, i)})
				}
			}
			_, err := e.Encode(m)
			if err != nil {
				fmt.Println(err)
			}
		})
		pw.Close()
	}()

	req, err := http.NewRequest(t.spec.Spec.Method, t.spec.Spec.Addr, pr)
	if err != nil {
		return err
	}

	if t.spec.Spec.Timeout <= 0 {
		ctx, cancel := context.WithTimeout(context.Background(), t.spec.Spec.Timeout)
		req = req.WithContext(ctx)
		defer cancel()
	}
	var resp *http.Response
	if t.spec.Spec.NoKeepAlive {
		resp, err = newOutPutClient().Do(req)
	} else {
		resp, err = outputHTTPKeepAliveClient.Do(req)

	}
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return req.Body.Close()
}

func (t *OutputHTTPTransformation) UpdateWatermark(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateWatermark(pt)
}
func (t *OutputHTTPTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
	return t.d.UpdateProcessingTime(pt)
}
func (t *OutputHTTPTransformation) Finish(id execute.DatasetID, err error) {
	t.d.Finish(err)
}
