package disksort

import (
	"bufio"
	"errors"
	"io"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/iox"
)

func defaultConfig() config {
	return config{
		chunkSize:     10000,
		sortBatchSize: 10,
	}
}

func New(scannerProvider func(io.Reader) *bufio.Scanner, emitterProvider func(io.Writer) *iox.Emitter, compare func(string, string) int, opts ...Option) (*Sorter, error) {
	if scannerProvider == nil {
		return nil, errors.New("nil scanner provider")
	} else if emitterProvider == nil {
		return nil, errors.New("nil emitter provider")
	} else if compare == nil {
		return nil, errors.New("nil comparator")
	}

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return &Sorter{
		cfg:             cfg,
		compare:         compare,
		scannerProvider: scannerProvider,
		emitterProvider: emitterProvider,
	}, nil
}

func validate(cfg config) error {
	switch {
	case cfg.chunkSize <= 0:
		return errors.New("chunk size must be positive")
	case cfg.sortBatchSize <= 1:
		return errors.New("sort batch size must be at least 2")
	default:
		return nil
	}
}
