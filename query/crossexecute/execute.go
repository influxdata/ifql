package crossexecute

import (
	"context"
	"errors"
	"io"

	"github.com/influxdata/ifql/query"
	"github.com/influxdata/ifql/query/control"
	"github.com/influxdata/ifql/query/execute"
)

type QueryTranspiler interface {
	Transpile(ctx context.Context, txt string) (*query.Spec, error)
}

type ResultWriter interface {
	WriteTo(w io.Writer, results map[string]execute.Result) error
	Encode( w io.Writer, result execute.Result)
}

type Querier interface {
	Query(ctx context.Context, spec *query.Spec) (*control.Query, error)
}

func CrossExecute(ctx context.Context, q string, querier Querier, transpiler QueryTranspiler, writer ResultWriter, w io.Writer) error {
	spec, err := transpiler.Transpile(ctx, q)
	if err != nil {
		return err
	}
	query, err := querier.Query(ctx, spec)
	if err != nil {
		return err
	}
	defer query.Done()

	select {
	case <-ctx.Done():
		return errors.New("context done before query completed")
	case results, ok := <-query.Ready():
		if !ok {
			return query.Err()
		}
		return writer.WriteTo(w, results)
	}
}
