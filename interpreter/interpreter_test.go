package interpreter_test

import (
	"errors"
	"regexp"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/ifql/ast"
	"github.com/influxdata/ifql/interpreter"
	"github.com/influxdata/ifql/parser"
	"github.com/influxdata/ifql/semantic"
	"github.com/influxdata/ifql/semantic/semantictest"
	"github.com/influxdata/ifql/values"
)

var testScope = interpreter.NewScope()
var testDeclarations = make(semantic.DeclarationScope)

func addFunc(f function) {
	testScope.Set(f.name, f)
	testDeclarations[f.name] = semantic.NewExternalVariableDeclaration(f.name, f.t)
}

func init() {
	addFunc(function{
		name: "fortyTwo",
		t: semantic.NewFunctionType(semantic.FunctionSignature{
			ReturnType: semantic.Float,
		}),
		call: func(args values.Object) (values.Value, error) {
			return values.NewFloatValue(42.0), nil
		},
	})
	addFunc(function{
		name: "six",
		t: semantic.NewFunctionType(semantic.FunctionSignature{
			ReturnType: semantic.Float,
		}),
		call: func(args values.Object) (values.Value, error) {
			return values.NewFloatValue(6.0), nil
		},
	})
	addFunc(function{
		name: "nine",
		t: semantic.NewFunctionType(semantic.FunctionSignature{
			ReturnType: semantic.Float,
		}),
		call: func(args values.Object) (values.Value, error) {
			return values.NewFloatValue(9.0), nil
		},
	})
	addFunc(function{
		name: "fail",
		t: semantic.NewFunctionType(semantic.FunctionSignature{
			ReturnType: semantic.Bool,
		}),
		call: func(args values.Object) (values.Value, error) {
			return nil, errors.New("fail")
		},
	})
	addFunc(function{
		name: "plusOne",
		t: semantic.NewFunctionType(semantic.FunctionSignature{
			Params:       map[string]semantic.Type{"x": semantic.Float},
			ReturnType:   semantic.Float,
			PipeArgument: "x",
		}),
		call: func(args values.Object) (values.Value, error) {
			v, ok := args.Get("x")
			if !ok {
				return nil, errors.New("missing argument x")
			}
			return values.NewFloatValue(v.Float() + 1), nil
		},
	})
}

// TestEval tests whether a program can run to completion or not
func TestEval(t *testing.T) {
	testCases := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{
			name:  "call function",
			query: "six()",
		},
		{
			name:    "call function with fail",
			query:   "fail()",
			wantErr: true,
		},
		{
			name:    "call function with duplicate args",
			query:   "plusOne(x:1.0, x:2.0)",
			wantErr: true,
		},
		{
			name:    "call function with missing args",
			query:   "plusOne()",
			wantErr: true,
		},
		{
			name: "reassign nested scope",
			query: `
			six = six()
			six()
			`,
			wantErr: true,
		},
		{
			name: "binary expressions",
			query: `
			six = six()
			nine = nine()

			answer = fortyTwo() == six * nine
			`,
		},
		{
			name: "logical expressions short circuit",
			query: `
            six = six()
            nine = nine()

            answer = (not (fortyTwo() == six * nine)) or fail()
			`,
		},
		{
			name: "arrow function",
			query: `
            plusSix = (r) => r + six()
            plusSix(r:1.0) == 7.0 or fail()
			`,
		},
		{
			name: "arrow function block",
			query: `
            f = (r) => {
                r2 = r * r
                return (r - r2) / r2
            }
            f(r:2.0) == -0.5 or fail()
			`,
		},
		{
			name: "arrow function with default param",
			query: `
            addN = (r,n=4) => r + n
            addN(r:2) == 6 or fail()
			addN(r:3,n:1) == 4 or fail()
			`,
		},
		{
			name: "scope closing",
			query: `
			x = 5
            plusX = (r) => r + x
            plusX(r:2) == 7 or fail()
			`,
		},
		{
			name: "scope closing mutable",
			query: `
			x = 5
            plusX = (r) => r + x
            plusX(r:2) == 7 or fail()
			x = 1
            plusX(r:2) == 3 or fail()
			`,
		},
		{
			name: "nested scope mutations not visible outside",
			query: `
			x = 5
            xinc = () => {
                x = x + 1
                return x
            }
            xinc() == 6 or fail()
            x == 5 or fail()
            x = 1
            xinc() == 2 or fail()
            `,
		},
		{
			name: "return map from func",
			query: `
            toMap = (a,b) => ({
                a: a,
                b: b,
            })
            m = toMap(a:1, b:false)
            m.a == 1 or fail()
            not m.b or fail()
			`,
		},
		{
			name: "pipe expression",
			query: `
			add = (a=<-,b) => a + b
			one = 1
			one |> add(b:2) == 3 or fail()
			`,
		},
		{
			name: "ignore pipe default",
			query: `
			add = (a=<-,b) => a + b
			add(a:1, b:2) == 3 or fail()
			`,
		},
		{
			name: "pipe expression function",
			query: `
			add = (a=<-,b) => a + b
			six() |> add(b:2.0) == 8.0 or fail()
			`,
		},
		{
			name: "pipe builtin function",
			query: `
			six() |> plusOne() == 7.0 or fail()
			`,
		},
		{
			name: "regex match",
			query: `
			"abba" =~ /^a.*a$/ or fail()
			`,
		},
		{
			name: "regex not match",
			query: `
			"abc" =~ /^a.*a$/ and fail()
			`,
		},
		{
			name: "not regex match",
			query: `
			"abc" !~ /^a.*a$/ or fail()
			`,
		},
		{
			name: "not regex not match",
			query: `
			"abba" !~ /^a.*a$/ and fail()
			`,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			program, err := parser.NewAST(tc.query)
			if err != nil {
				t.Fatal(err)
			}
			graph, err := semantic.New(program, testDeclarations.Copy())
			if err != nil {
				t.Fatal(err)
			}

			err = interpreter.Eval(graph, testScope.Nest())
			if !tc.wantErr && err != nil {
				t.Fatal(err)
			} else if tc.wantErr && err == nil {
				t.Fatal("expected error")
			}
		})
	}

}
func TestResolver(t *testing.T) {
	var got semantic.Expression
	scope := interpreter.NewScope()
	declarations := make(semantic.DeclarationScope)
	f := function{
		name: "resolver",
		t: semantic.NewFunctionType(semantic.FunctionSignature{
			Params: map[string]semantic.Type{
				"f": semantic.NewFunctionType(semantic.FunctionSignature{
					Params: map[string]semantic.Type{"r": semantic.Int},
				}),
			},
			ReturnType: semantic.Int,
		}),
		call: func(args values.Object) (values.Value, error) {
			f, ok := args.Get("f")
			if !ok {
				return nil, errors.New("missing argument f")
			}
			resolver, ok := f.Function().(interpreter.Resolver)
			if !ok {
				return nil, errors.New("function cannot be resolved")
			}
			g, err := resolver.Resolve()
			if err != nil {
				return nil, err
			}
			got = g.(semantic.Expression)
			return nil, nil
		},
	}
	scope.Set(f.name, f)
	declarations[f.name] = semantic.NewExternalVariableDeclaration(f.name, f.t)

	program, err := parser.NewAST(`
	x = 42
	resolver(f: (r) => r + x)
`)
	if err != nil {
		t.Fatal(err)
	}

	graph, err := semantic.New(program, testDeclarations)
	if err != nil {
		t.Fatal(err)
	}

	if err := interpreter.Eval(graph, scope); err != nil {
		t.Fatal(err)
	}

	want := &semantic.FunctionExpression{
		Params: []*semantic.FunctionParam{{Key: &semantic.Identifier{Name: "r"}}},
		Body: &semantic.BinaryExpression{
			Operator: ast.AdditionOperator,
			Left:     &semantic.IdentifierExpression{Name: "r"},
			Right:    &semantic.IntegerLiteral{Value: 42},
		},
	}
	if !cmp.Equal(want, got, semantictest.CmpOptions...) {
		t.Errorf("unexpected resoved function: -want/+got\n%s", cmp.Diff(want, got, semantictest.CmpOptions...))
	}
	if wt, gt := want.Type(), got.Type(); wt != gt {
		t.Errorf("unexpected resoved function types: want: %v got: %v", wt, gt)
	}
}

type function struct {
	name string
	t    semantic.Type
	call func(args values.Object) (values.Value, error)
}

func (f function) Type() semantic.Type {
	return f.t
}

func (f function) Str() string {
	panic(values.UnexpectedKind(semantic.Object, semantic.String))
}
func (f function) Int() int64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Int))
}
func (f function) UInt() uint64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.UInt))
}
func (f function) Float() float64 {
	panic(values.UnexpectedKind(semantic.Object, semantic.Float))
}
func (f function) Bool() bool {
	panic(values.UnexpectedKind(semantic.Object, semantic.Bool))
}
func (f function) Time() values.Time {
	panic(values.UnexpectedKind(semantic.Object, semantic.Time))
}
func (f function) Duration() values.Duration {
	panic(values.UnexpectedKind(semantic.Object, semantic.Duration))
}
func (f function) Regexp() *regexp.Regexp {
	panic(values.UnexpectedKind(semantic.Object, semantic.Regexp))
}
func (f function) Array() values.Array {
	panic(values.UnexpectedKind(semantic.Object, semantic.Function))
}
func (f function) Object() values.Object {
	panic(values.UnexpectedKind(semantic.Object, semantic.Object))
}
func (f function) Function() values.Function {
	return f
}

func (f function) Call(args values.Object) (values.Value, error) {
	return f.call(args)
}
