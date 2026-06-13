package envx

import (
	"errors"
	"strconv"
)

type PositiveInt struct {
	Value int
}

func PositiveIntParserFunc(s string) (interface{}, error) {
	i, err := strconv.Atoi(s)
	if err != nil {
		return nil, err
	}

	if i <= 0 {
		return nil, errors.New("must be positive")
	}

	return PositiveInt{i}, nil
}
