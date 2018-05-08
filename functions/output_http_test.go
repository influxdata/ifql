package functions_test

import (
	"testing"

	"github.com/influxdata/ifql/functions"
	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/querytest"
)

func TestOutputHTTP_Marshaling(t *testing.T) {
	data := []byte(`{"id":"outputHTTP", "kind":"outputHTTP"}`)
	op := &query.Operation{
		ID:   "outputHTTP",
		Spec: &functions.OutputHTTPOpSpec{},
	}
	querytest.OperationMarshalingTestHelper(t, data, op)
}

func TestOutputHTTP_NewQuery(t *testing.T) {
}
