package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/flagx"
)

var (
	// server
	name          string
	port          int
	masterAddr    string
	advertiseAddr string
	initTimeout   time.Duration
	// heartbeat
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	// nfs
	nfsRoot = flagx.DirValue{Path: "/mnt/nfs"}
	// process
	mapTimeout time.Duration
)

func parseFlags() {
	flag.StringVar(&name, "name", "", "the worker's identity, leave out to generate a random one")
	flag.IntVar(&port, "port", 5000, "the port to listen on")
	flag.StringVar(&masterAddr, "master-address", "", "master address")
	flag.StringVar(&advertiseAddr, "advertise-address", "", "advertise address")
	flag.DurationVar(&initTimeout, "init-timeout", 30*time.Second, "init timeout")
	flag.DurationVar(&heartbeatInterval, "heartbeat-interval", 5*time.Second, "heartbeat interval")
	flag.DurationVar(&heartbeatTimeout, "heartbeat-timeout", 3*time.Second, "heartbeat timeout")
	flag.Var(&nfsRoot, "nfs-root", "NFS root directory")
	flag.DurationVar(&mapTimeout, "map-timeout", 60*time.Second, "timeout duration of map task")

	flag.Parse()
}

func validateArguments() error {
	switch {
	case port <= 0:
		return errors.New("invalid port")
	case len(masterAddr) == 0:
		return require("master address")
	case initTimeout <= 0:
		return invalidDuration("init timeout")
	case heartbeatInterval <= 0:
		return invalidDuration("heartbeat interval")
	case heartbeatTimeout <= 0:
		return invalidDuration("heartbeat timeout")
	case len(nfsRoot.Path) == 0:
		return require("NFS root")
	case mapTimeout <= 0:
		return invalidDuration("map timeout")
	default:
		return nil
	}
}

func invalidDuration(name string) error {
	return fmt.Errorf("invalid duration for %s", name)
}

func require(name string) error {
	return fmt.Errorf("%s is required", name)
}

func prepArguments() error {
	parseFlags()
	return validateArguments()
}
