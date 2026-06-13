package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/caarlos0/env/v11"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/envx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/fsx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/logx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/master"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/server"
	rpcv1 "github.com/nlduy0310/simple-distributed-mapreduce/rpc/v1"
)

type ServerConfig struct {
	Port          envx.Port    `env:"PORT" envDefault:"8000"`
	AdvertiseAddr envx.Address `env:"ADVERTISE_ADDR,required,notEmpty"`
}

type ServiceConfig struct {
	HealthyDuration     envx.PositiveDuration `env:"HEALTHY_DURATION" envDefault:"30s"`
	HealthcheckInterval envx.PositiveDuration `env:"HEALTHCHECK_INTERVAL" envDefault:"5s"`
	MapTimeout          envx.PositiveDuration `env:"MAP_TIMEOUT" envDefault:"60s"`
}

type Config struct {
	Svr          ServerConfig     `envPrefix:"SERVER_"`
	Svc          ServiceConfig    `envPrefix:"SVC_"`
	NfsRoot      envx.Directory   `env:"NFS_ROOT,notEmpty" envDefault:"/mnt/nfs"`
	InputPattern string           `env:"INPUT_PATTERN,required,notEmpty"`
	MaxWorkers   envx.PositiveInt `env:"MAX_WORKERS" envDefault:"100"`
}

func run() int {
	cfg := env.Must(env.ParseAsWithOptions[Config](envx.Options))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svrConfig, err := server.NewConfig(int(cfg.Svr.Port), string(cfg.Svr.AdvertiseAddr))
	if err != nil {
		logx.Err(errx.WithContext(err, "initialize config"))
		return 1
	}

	svr, err := server.New(svrConfig)
	if err != nil {
		logx.Err(errx.WithContext(err, "configure server"))
		return 1
	}
	defer svr.Close()

	inputFiles, err := findInputFiles(cfg.NfsRoot.Path, cfg.InputPattern)
	if err != nil {
		logx.Err(errx.WithContext(err, "find input files"))
		return 1
	}

	svcConfig := master.Config{
		InputFiles: inputFiles,
		MaxWorkers: cfg.MaxWorkers.Value,
		MapTimeout: cfg.Svc.MapTimeout.Value,
	}
	svc, err := master.NewService(svcConfig)
	if err != nil {
		logx.Err(errx.WithContext(err, "configure service"))
		return 1
	}

	rpcv1.RegisterMasterServiceServer(svr.GrpcServer, svc)

	go func() {
		svc.PeriodicHealthcheck(ctx, cfg.Svc.HealthcheckInterval.Value, cfg.Svc.HealthyDuration.Value)
	}()
	go func() {
		svc.RunAssignLoop(ctx)
	}()

	if err := svr.Serve(ctx); err != nil {
		logx.Err(errx.WithContext(err, "exited with error"))
		return 1
	}

	return 0
}

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

func main() {
	os.Exit(run())
}
