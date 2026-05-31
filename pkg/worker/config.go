package worker

import (
	"errors"
	"fmt"
	"time"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/validate"
)

var (
	errEmptyMasterAddr    = errors.New("empty master address")
	errEmptyAdvertiseAddr = errors.New("empty advertise address")
)

type Config struct {
	Name          string
	MasterAddr    string
	AdvertiseAddr string
	NfsRoot       string
	MapTimeout    time.Duration
}

func validateConfig(cfg Config) error {
	if len(cfg.MasterAddr) == 0 {
		return errEmptyMasterAddr
	} else if len(cfg.AdvertiseAddr) == 0 {
		return errEmptyAdvertiseAddr
	} else if err := validate.EnsureIsDir(cfg.NfsRoot); err != nil {
		return errx.WithContext(err, fmt.Sprintf("nfs root dir %s", cfg.NfsRoot))
	} else if cfg.MapTimeout <= 0 {
		return errors.New("invalid map timeout duration")
	}

	return nil
}
