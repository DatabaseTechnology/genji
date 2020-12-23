package stream

import (
	"errors"

	"github.com/genjidb/genji/sql/query/expr"
)

// ErrStreamClosed is used to indicate that a stream must be closed.
var ErrStreamClosed = errors.New("stream closed")

// An Iterator can iterate over values.
type Iterator interface {
	// Iterate goes through all the values and calls the given function by passing each one of them.
	// If the given function returns an error, the iteration stops.
	Iterate(fn func(env *expr.Environment) error) error
}

// An Operator is used to modify a stream.
// If an operator returns a value, it will be passed to the next operator.
// If it returns a nil value, the value will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by this function, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// Stream operators can be reused, and thus, any state or side effect should be kept within the operator closure
// unless the nature of the operator prevents that.
type Operator func() func(env *expr.Environment) (*expr.Environment, error)

type Stream struct {
	it Iterator
	op Operator
}

func New(it Iterator) Stream {
	return Stream{
		it: it,
	}
}

// Pipe creates a new Stream who can read its data from s and apply
// op to every document passed by its Iterate method.
func (s Stream) Pipe(op Operator) Stream {
	return Stream{
		it: s,
		op: op,
	}
}

// Iterate calls the underlying iterator's iterate method.
// If this stream was created using the Pipe method, it will apply fn
// to any document passed by the underlying iterator.
// If fn returns a document, it will be passed to the next stream.
// If it returns a nil document, the document will be ignored.
// If it returns an error, the stream will be interrupted and that error will bubble up
// and returned by fn, unless that error is ErrStreamClosed, in which case
// the Iterate method will stop the iteration and return nil.
// It implements the Iterator interface.
func (s Stream) Iterate(fn func(env *expr.Environment) error) error {
	if s.it == nil {
		return nil
	}

	if s.op == nil {
		return s.it.Iterate(fn)
	}

	opFn := s.op()

	err := s.it.Iterate(func(env *expr.Environment) error {
		env, err := opFn(env)
		if err != nil {
			return err
		}

		return fn(env)
	})
	if err != ErrStreamClosed {
		return err
	}

	return nil
}

type MapOperator struct {
	E expr.Expr
}

func Map(e expr.Expr) Operator {
	return (&MapOperator{E: e}).Operator
}

func (m *MapOperator) Operator() func(env *expr.Environment) (*expr.Environment, error) {
	var newEnv expr.Environment

	return func(env *expr.Environment) (*expr.Environment, error) {
		v, err := m.E.Eval(env)
		if err != nil {
			return nil, err
		}

		newEnv.SetCurrentValue(v)
		newEnv.Outer = env
		return &newEnv, nil
	}
}
