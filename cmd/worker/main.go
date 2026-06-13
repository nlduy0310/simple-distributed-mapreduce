package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/envx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/logx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/server"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/worker"
	rpcv1 "github.com/nlduy0310/simple-distributed-mapreduce/rpc/v1"
)

type Config struct {
	Svr     ServerConfig   `envPrefix:"SERVER_"`
	Svc     ServiceConfig  `envPrefix:"SVC_"`
	Name    string         `env:"NAME,required"`
	NfsRoot envx.Directory `env:"NFS_ROOT,notEmpty" envDefault:"/mnt/nfs"`
}

type ServerConfig struct {
	Port          envx.Port    `env:"PORT,notEmpty" envDefault:"5000"`
	AdvertiseAddr envx.Address `env:"ADVERTISE_ADDR,required,notEmpty"`
}

type ServiceConfig struct {
	MasterAddr        envx.Address          `env:"MASTER_ADDR,required,notEmpty"`
	InitTimeout       envx.PositiveDuration `env:"INIT_TIMEOUT,notEmpty" envDefault:"30s"`
	HeartbeatInterval envx.PositiveDuration `env:"HEARTBEAT_INTERVAL,notEmpty" envDefault:"5s"`
	HeartbeatTimeout  envx.PositiveDuration `env:"HEARTBEAT_TIMEOUT,notEmpty" envDefault:"3s"`
	MapTimeout        envx.PositiveDuration `env:"MAP_TIMEOUT,notEmpty" envDefault:"60s"`
}

func run() error {
	cfg, err := env.ParseAsWithOptions[Config](envx.Options)
	if err != nil {
		return err
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancelWithCause := context.WithCancelCause(signalCtx)
	defer cancelWithCause(nil)

	svrConfig, err := server.NewConfig(int(cfg.Svr.Port), string(cfg.Svr.AdvertiseAddr))
	if err != nil {
		return errx.WithContext(err, "init server config")
	}

	svr, err := server.New(svrConfig)
	if err != nil {
		return errx.WithContext(err, "configure server")
	}
	defer svr.Close()

	svc, err := worker.NewService(worker.Config{
		Name:          cfg.Name,
		MasterAddr:    string(cfg.Svc.MasterAddr),
		AdvertiseAddr: string(cfg.Svr.AdvertiseAddr),
		NfsRoot:       cfg.NfsRoot.Path,
		MapTimeout:    cfg.Svc.MapTimeout.Value,
	})
	if err != nil {
		return errx.WithContext(err, "configure service")
	}
	defer svc.Close()

	initCtx, timeoutInit := context.WithTimeout(ctx, cfg.Svc.InitTimeout.Value)
	defer timeoutInit()
	if err = svc.Init(initCtx); err != nil {
		return errx.WithContext(err, "initialize service")
	}

	rpcv1.RegisterWorkerServiceServer(svr.GrpcServer, svc)

	go func() {
		err := periodicHeartbeat(ctx, svc, cfg.Svc.HeartbeatInterval.Value, cfg.Svc.HeartbeatTimeout.Value)
		if err != nil {
			cancelWithCause(errx.WithContext(err, "heartbeat failure"))
		}
	}()

	if err := svr.Serve(ctx); err != nil {
		return errx.WithContext(err, "server exited with error")
	}

	return nil
}

func periodicHeartbeat(ctx context.Context, svc *worker.Service, interval time.Duration, timeout time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := heartbeatWithTimeout(ctx, svc, timeout); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func heartbeatWithTimeout(parent context.Context, svc *worker.Service, timeout time.Duration) error {
	ctx, timeoutFn := context.WithTimeout(parent, timeout)
	defer timeoutFn()
	if err := svc.DoHeartbeat(ctx); err != nil {
		return err
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		logx.Err(err)
		os.Exit(1)
	}
}
