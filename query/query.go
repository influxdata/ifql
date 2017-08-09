package query

import (
	"errors"
	"fmt"

	"github.com/influxdata/ifql/query/execute/storage"
)

// QuerySpec specifies a query.
type QuerySpec struct {
	Operations []*Operation
	Edges      []Edge

	sorted   []*Operation
	children map[OperationID][]*Operation
	parents  map[OperationID][]*Operation
}

type Edge struct {
	Parent OperationID `json:"parent"`
	Child  OperationID `json:"child"`
}

// TODO: instead of taking a predicate, take what can be returned by
//       the parser. That is, the parser could have variable references.
type WhereExpressionSpec struct {
	Predicate *storage.Predicate
}

// TODO: wire this up to evaluate in the engine to fill in variables
//       with values
func (w *WhereExpressionSpec) Evaluate() *storage.Predicate {
	return w.Predicate
}

// ExpressionSpec specifies an expression.
// Expressions may return any value that the storage supports.
//TODO flesh this out.
type ExpressionSpec interface{}

// Walk calls f on each operation exactly once.
// The function f will be called on an operation only after
// all of its parents have already been passed to f.
func (q *QuerySpec) Walk(f func(o *Operation) error) error {
	if len(q.sorted) == 0 {
		if err := q.prepare(); err != nil {
			return err
		}
	}
	for _, o := range q.sorted {
		err := f(o)
		if err != nil {
			return err
		}
	}
	return nil
}

// Validate ensures the query is a valid DAG.
func (q *QuerySpec) Validate() error {
	return q.prepare()
}

// Children returns a list of children for a given operation.
// If the query is invalid no children will be returned.
func (q *QuerySpec) Children(id OperationID) []*Operation {
	if q.children == nil {
		err := q.prepare()
		if err != nil {
			return nil
		}
	}
	return q.children[id]
}

// Parents returns a list of parents for a given operation.
// If the query is invalid no parents will be returned.
func (q *QuerySpec) Parents(id OperationID) []*Operation {
	if q.parents == nil {
		err := q.prepare()
		if err != nil {
			return nil
		}
	}
	return q.parents[id]
}

// prepare populates the internal datastructure needed to quickly navigate the query DAG.
// As a result the query DAG is validated.
func (q *QuerySpec) prepare() error {
	parents, children, roots, err := q.determineParentsChildrenAndRoots()
	if err != nil {
		return err
	}
	if len(roots) == 0 {
		return errors.New("query has no root nodes")
	}

	q.parents = parents
	q.children = children

	tMarks := make(map[OperationID]bool)
	pMarks := make(map[OperationID]bool)

	for _, r := range roots {
		if err := q.visit(tMarks, pMarks, r); err != nil {
			return err
		}
	}
	//reverse q.sorted
	for i, j := 0, len(q.sorted)-1; i < j; i, j = i+1, j-1 {
		q.sorted[i], q.sorted[j] = q.sorted[j], q.sorted[i]
	}
	return nil
}

func (q *QuerySpec) computeLookup() (map[OperationID]*Operation, error) {
	lookup := make(map[OperationID]*Operation, len(q.Operations))
	for _, o := range q.Operations {
		if _, ok := lookup[o.ID]; ok {
			return nil, fmt.Errorf("found duplicate operation ID %q", o.ID)
		}
		lookup[o.ID] = o
	}
	return lookup, nil
}

func (q *QuerySpec) determineParentsChildrenAndRoots() (parents, children map[OperationID][]*Operation, roots []*Operation, _ error) {
	lookup, err := q.computeLookup()
	if err != nil {
		return nil, nil, nil, err
	}
	children = make(map[OperationID][]*Operation, len(q.Operations))
	parents = make(map[OperationID][]*Operation, len(q.Operations))
	for _, e := range q.Edges {
		// Build children map
		c, ok := lookup[e.Child]
		if !ok {
			return nil, nil, nil, fmt.Errorf("edge references unknown child operation %q", e.Child)
		}
		children[e.Parent] = append(children[e.Parent], c)

		// Build parents map
		p, ok := lookup[e.Parent]
		if !ok {
			return nil, nil, nil, fmt.Errorf("edge references unknown parent operation %q", e.Parent)
		}
		parents[e.Child] = append(parents[e.Child], p)
	}
	// Find roots, i.e operations with not parents.
	for _, o := range q.Operations {
		if len(parents[o.ID]) == 0 {
			roots = append(roots, o)
		}
	}
	return
}

// Depth first search topological sorting of a DAG.
// https://en.wikipedia.org/wiki/Topological_sorting#Algorithms
func (q *QuerySpec) visit(tMarks, pMarks map[OperationID]bool, o *Operation) error {
	id := o.ID
	if tMarks[id] {
		return errors.New("found cycle in query")
	}

	if !pMarks[id] {
		tMarks[id] = true
		for _, c := range q.children[id] {
			if err := q.visit(tMarks, pMarks, c); err != nil {
				return err
			}
		}
		pMarks[id] = true
		tMarks[id] = false
		q.sorted = append(q.sorted, o)
	}
	return nil
}