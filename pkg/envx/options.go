package envx

import (
	"reflect"

	"github.com/caarlos0/env/v11"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/zero"
)

var Options env.Options = env.Options{
	FuncMap: map[reflect.Type]env.ParserFunc{
		reflect.TypeOf(zero.Of[Port]()):             PortParserFunc,
		reflect.TypeOf(zero.Of[Address]()):          AddressParserFunc,
		reflect.TypeOf(zero.Of[PositiveDuration]()): PositiveDurationParserFunc,
		reflect.TypeOf(zero.Of[Directory]()):        DirectoryParserFunc,
		reflect.TypeOf(zero.Of[PositiveInt]()):      PositiveIntParserFunc,
	},
}
