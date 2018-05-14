package functions

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"time"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/execute"
	"github.com/influxdata/ifql/query/plan"
	"github.com/influxdata/line-protocol"
	"github.com/pkg/errors"
)

const OutputHTTPKind = "outputHTTP"

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
	Method      string            `json:"method"` // default behavior should be POST
	Headers     map[string]string `json:"headers"`
	URLParams   map[string]string `json:"urlparams"`
	Timeout     time.Duration     `json:"timeout"` // default to something reasonable if zero
	NoKeepAlive bool              `json:"nokeepalive"`
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
	query.RegisterFunction(OutputHTTPKind, createCountOpSpec, outputHTTPSignature)
	query.RegisterOpSpec(OutputHTTPKind,
		func() query.OperationSpec { return &OutputHTTPOpSpec{} })
	plan.RegisterProcedureSpec(OutputHTTPKind, newOutputHTTPProcedure, OutputHTTPKind)
	//execute.RegisterTransformation(OutputHTTPKind, createCountTransformation)
}

func (OutputHTTPOpSpec) Kind() query.OperationKind {
	return OutputHTTPKind
}

type OutputHTTPProcedureSpec struct {
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
	return &OutputHTTPProcedureSpec{}, nil
}

type OutputHTTPTransformation struct {
	d      execute.Dataset
	cache  execute.BlockBuilderCache
	bounds execute.Bounds
	spec   *OutputHTTPProcedureSpec
}

func (t *OutputHTTPTransformation) RetractBlock(id execute.DatasetID, meta execute.BlockMetadata) error {
	key := execute.ToBlockKey(meta)
	return t.d.RetractBlock(key)
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

	var w io.Writer
	m := httpOutputMetric{}
	e := protocol.NewEncoder(w)
	e.FailOnFieldErr(true)
	m.setTags(map[string]string(b.Tags()))
	cols := b.Cols()
	labels := make(map[string]idxType, len(cols))
	for i, col := range cols {
		labels[col.Label] = idxType{Idx: i, Type: col.Type.String()}
	}
	// do time
	timeColLabel := execute.TimeColLabel //TODO: allow for user supplied col labels
	timeColIdx, ok := labels[timeColLabel]
	if !ok {
		return errors.New("Could not get time column")
	}
	if timeColIdx.Type != execute.TTime.String() {
		return fmt.Errorf("column %s is not of type %s", timeColLabel, timeColIdx.Type)
	}
	iter := b.Col(timeColIdx.Idx)
	iter.DoTime(func(t []execute.Time, r execute.RowReader) {
		log.Print(t)
	})
	return nil
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
