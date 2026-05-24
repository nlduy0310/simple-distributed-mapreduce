package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/flagx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/fsx"
)

var (
	// server
	port          int
	advertiseAddr string
	// healthcheck
	healthyDuration     time.Duration
	healthcheckInterval time.Duration
	// input
	nfsRoot      = flagx.DirValue{Path: "/mnt/nfs"}
	inputPattern string
	// operations
	maxWorkers int
	mapTimeout time.Duration
)

func prepArguments() {
	flag.IntVar(&port, "port", 8000, "grpc server port")
	flag.StringVar(&advertiseAddr, "advertise-address", "", "grpc server advertise address")

	flag.DurationVar(&healthyDuration, "healthy-duration", 30*time.Second, "maximum duration since last heartbeat of a healthy worker")
	flag.DurationVar(&healthcheckInterval, "healthcheck-interval", 5*time.Second, "interval between worker heartbeat checks")

	flag.Var(&nfsRoot, "nfs-root", "NFS root volume")
	flag.StringVar(&inputPattern, "input", "", "input files glob pattern relative to NFS root")

	flag.IntVar(&maxWorkers, "max-workers", 100, "maximum number of workers")
	flag.DurationVar(&mapTimeout, "map-timeout", 60*time.Second, "timeout of map task")

	flag.Parse()

	if err := validateArguments(); err != nil {
		exit1(errx.WithContext(err, "validate arguments"))
	}
}

func validateArguments() error {
	switch {
	case port <= 0:
		return errors.New("invalid port")
	case len(advertiseAddr) == 0:
		return require("advertise address")
	case len(nfsRoot.Path) == 0:
		return require("NFS root")
	case len(inputPattern) == 0:
		return require("input pattern")
	case maxWorkers <= 0:
		return invalid("max workers")
	case mapTimeout <= 0:
		return invalid("map timeout")
	default:
		return nil
	}
}

func require(name string) error {
	return fmt.Errorf("%s must be provided", name)
}

func invalid(name string) error {
	return fmt.Errorf("invalid %s", name)
}

func exit1(err error) {
	log.Println(err.Error())
	os.Exit(1)
}

// findInputFiles glob files in a directory, returning relative paths
func findInputFiles(nfsRoot string, relPattern string) ([]string, error) {
	absPattern := filepath.Join(nfsRoot, relPattern)
	matches, err := filepath.Glob(absPattern)
	if err != nil {
		return nil, errx.WithContext(err, "glob input files")
	}

	for _, match := range matches {
		is, err := fsx.IsFile(match)
		if err != nil {
			return nil, errx.WithContext(err, fmt.Sprintf("validate path %s", match))
		} else if !is {
			return nil, errx.WithContext(fsx.ErrNotAFile, fmt.Sprintf("validate path %s", match))
		}
	}

	for i := range matches {
		matches[i], _ = filepath.Rel(nfsRoot, matches[i])
	}

	return matches, nil
}
