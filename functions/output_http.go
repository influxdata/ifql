package functions

import (
	"log"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/plan"
)

const OutputHTTPKind = "outputHTTP"

var outputHTTPUserAgent = "ifqld/dev"

type OutputHTTPOpSpec struct {
	Addr    string
	Method  string
	headers map[string]string
}

var outputHTTPSignature = query.DefaultFunctionSignature()

func init() {
	log.Println("********************************", outputHTTPUserAgent, "\n********************************************")
	query.RegisterFunction(OutputHTTPKind, createCountOpSpec, countSignature)
	query.RegisterOpSpec(OutputHTTPKind,
		func() query.OperationSpec { return &OutputHTTPOpSpec{} })
	plan.RegisterProcedureSpec(OutputHTTPKind, newOutputHTTPProcedure, OutputHTTPKind)
	//execute.RegisterTransformation(CountKind, createCountTransformation)
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
	return o
}

func newOutputHTTPProcedure(qs query.OperationSpec, a plan.Administration) (plan.ProcedureSpec, error) {
	//spec, ok := qs.(*OutputHTTPOpSpec)
	// if !ok {
	// 	return nil, fmt.Errorf("invalid spec type %T", qs)
	// }
	return &OutputHTTPProcedureSpec{}, nil
}
