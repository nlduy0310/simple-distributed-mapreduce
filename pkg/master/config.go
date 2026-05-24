package master

import (
	"errors"
	"time"
)

type Config struct {
	InputFiles []string // paths relative to NFS root
	MaxWorkers int
	MapTimeout time.Duration
}

func validateConfig(cfg Config) error {
	switch {
	case len(cfg.InputFiles) == 0:
		return errors.New("no input files")
	case cfg.MaxWorkers <= 0:
		return errors.New("invalid number of max workers")
	case cfg.MapTimeout <= 0:
		return errors.New("invalid map timeout")
	default:
		return nil
	}
}
