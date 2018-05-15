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
	"sync"
	"time"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
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
	Addr        string            `json:"addr"`
	Method      string            `json:"method"`    // default behavior should be POST
	Headers     map[string]string `json:"headers"`   // TODO: implement Headers after bug with keys and arrays and objects is fixed (new parser implemented, with string literals as keys)
	URLParams   map[string]string `json:"urlparams"` // TODO: implement URLParams after bug with keys and arrays and objects is fixed (new parser implemented, with string literals as keys)
	Timeout     time.Duration     `json:"timeout"`   // default to something reasonable if zero
	NoKeepAlive bool              `json:"nokeepalive"`
	TimeColumn  string            `json:"time_column"`
	// TagColumns   []string          `json:"tag_columns"`
	// ValueColumns []string          `json:"value_columns"`
}

func (o *OutputHTTPOpSpec) ReadArgs(args query.Arguments) error {
	var err error
	o.Addr, err = args.GetRequiredString("addr")
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

	// tagColumns, ok, err = args.GetArray("tag_columns", semantic.String)
	// if err != nil {
	// 	return err
	// }
	// if !ok {
	// 	o.TagColumns = nil
	// }

	// o.TimeColumn, ok, err = args.GetString("value_columns")
	// if err != nil {
	// 	return err
	// }
	// if !ok {
	// 	o.TimeColumn = execute.TimeColLabel
	// }

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

// UnmarshalJSON unmarshals and validates OutputHTTPOpSpec
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
	fmt.Println(u)
	return nil
}

var outputHTTPSignature = query.DefaultFunctionSignature()

func init() {
	query.RegisterFunction(OutputHTTPKind, createOutputHTTPOpSpec, outputHTTPSignature)
	query.RegisterOpSpec(OutputHTTPKind,
		func() query.OperationSpec { return &OutputHTTPOpSpec{} })
	plan.RegisterProcedureSpec(OutputHTTPKind, newOutputHTTPProcedure, OutputHTTPKind)
	//execute.RegisterTransformation(OutputHTTPKind, createCountTransformation)
}

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

type OutputHTTPTransformation struct {
	d     execute.Dataset
	cache execute.BlockBuilderCache
	//bounds execute.Bounds
	spec *OutputHTTPProcedureSpec
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

func (m *httpOutputMetric) Tags() []*protocol.Tag {
	return m.tags
}
func (m *httpOutputMetric) Fields() []*protocol.Tag {
	return m.tags
}

func (m *httpOutputMetric) Name() string {
	return m.name
}

func (m *httpOutputMetric) AddTag(k, v string) {
	panic("JUST IMPLEMENTED TO SATISFY API")
}
func (m *httpOutputMetric) AddField(k string, v interface{}) {
	panic("JUST IMPLEMENTED TO SATISFY API")
}

func (m *httpOutputMetric) setTags(tags map[string]string) {
	//prealocate the tags
	m.tags = m.tags[:0]
	tagsSlab := make([]protocol.Tag, len(tags))
	//m.tags = make([]*protocol.Tag, 0, len(tags))
	i := 0
	for k, v := range tags {
		tagsSlab[i].Key = k
		tagsSlab[i].Value = v
		m.tags = append(m.tags, &tagsSlab[i])
	}
	sort.Slice(m.tags, func(i, j int) bool { return m.tags[i].Key < m.tags[j].Key })
}

type idxType struct {
	Idx  int
	Type string
}

func (t *OutputHTTPTransformation) Process(id execute.DatasetID, b execute.Block) error {
	pr, pw := io.Pipe()
	m := httpOutputMetric{}
	e := protocol.NewEncoder(pw)
	e.FailOnFieldErr(true)
	e.SetFieldSortOrder(protocol.SortFields)
	m.setTags(map[string]string(b.Tags()))
	cols := b.Cols()
	labels := make(map[string]idxType, len(cols))
	for i, col := range cols {
		labels[col.Label] = idxType{Idx: i, Type: col.Type.String()}
	}
	// do time
	timeColLabel := t.spec.Spec.TimeColumn
	timeColIdx, ok := labels[timeColLabel]
	if !ok {
		return errors.New("Could not get time column")
	}
	if timeColIdx.Type != execute.TTime.String() {
		return fmt.Errorf("column %s is not of type %s", timeColLabel, timeColIdx.Type)
	}
	iter := b.Col(timeColIdx.Idx)
	wg := &sync.WaitGroup{}

	go func() {
		iter.DoTime(func(t []execute.Time, er execute.RowReader) {

			enc
		})
	}()
	req, err := http.NewRequest(t.spec.Spec.Method, t.spec.Spec.Addr, pr)
	if err != nil {
		return err
	}

	//fmt.Println(buf.Len())
	//req.Body = ioutil.Close

	if t.spec.Spec.Timeout <= 0 {
		ctx, cancel := context.WithTimeout(context.Background(), t.spec.Spec.Timeout)
		req = req.WithContext(ctx)
		defer cancel()
	}
	resp, err := outputHTTPKeepAliveClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return req.Body.Close()
	//it := b.Times()
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
