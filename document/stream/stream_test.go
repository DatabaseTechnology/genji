package stream_test

import (
	"fmt"
	"testing"

	"github.com/genjidb/genji/document/stream"
	"github.com/genjidb/genji/sql/parser"
	"github.com/genjidb/genji/sql/query/expr"
)

func TestStream(t *testing.T) {
	s := stream.New()

	s = s.Pipe(stream.Map(parser.MustParseExpr("a")))

	s.Iterate(func(env *expr.Environment) error {
		v, ok := env.GetCurrentValue()
		fmt.Println(v, ok)
		return nil
	})
}
