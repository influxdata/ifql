package plan

import "github.com/influxdata/ifql/query"

type AbstractOperation interface {
	Parents() []AbstractDataset
	Children() []AbstractDataset
	Operation() *query.Operation
}
type absOp struct {
	parents   []AbstractDataset
	children  []AbstractDataset
	operation *query.Operation
}

func (o *absOp) Parents() []AbstractDataset {
	return o.parents
}
func (o *absOp) Children() []AbstractDataset {
	return o.children
}
func (o *absOp) Operation() *query.Operation {
	return o.operation
}

type clearOperation struct {
	operation
}
