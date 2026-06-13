package envx

import (
	"errors"
	"strconv"
)

type Port int

func PortParserFunc(s string) (interface{}, error) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return nil, errors.New("port must be an int")
	}

	if !(v >= 1 && v <= 65535) {
		return nil, errors.New("port must be in range [1, 65535]")
	}

	return Port(v), nil
}
