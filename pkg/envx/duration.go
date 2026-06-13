package envx

import (
	"errors"
	"time"
)

type PositiveDuration struct {
	Value time.Duration
}

func PositiveDurationParserFunc(s string) (interface{}, error) {
	duration, err := time.ParseDuration(s)
	if err != nil {
		return nil, err
	}

	if duration <= 0 {
		return nil, errors.New("non-positive duration")
	}

	return PositiveDuration{duration}, nil
}
