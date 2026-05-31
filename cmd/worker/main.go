package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/logx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/server"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/worker"
	rpcv1 "github.com/nlduy0310/simple-distributed-mapreduce/rpc/v1"
)

func run() error {
	flag.Parse()

	if err := validateArguments(); err != nil {
		return errx.WithContext(err, "configure application")
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancelWithCause := context.WithCancelCause(signalCtx)
	defer cancelWithCause(nil)

	svrConfig, err := server.NewConfig(port, advertiseAddr)
	if err != nil {
		return errx.WithContext(err, "init server config")
	}

	svr, err := server.New(svrConfig)
	if err != nil {
		return errx.WithContext(err, "configure server")
	}
	defer svr.Close()

	svc, err := worker.NewService(worker.Config{
		Name:          name,
		MasterAddr:    masterAddr,
		AdvertiseAddr: svr.Config.AdvertiseAddr,
		NfsRoot:       nfsRoot.Path,
		MapTimeout:    mapTimeout,
	})
	if err != nil {
		return errx.WithContext(err, "configure service")
	}
	defer svc.Close()

	initCtx, timeoutInit := context.WithTimeout(ctx, initTimeout)
	defer timeoutInit()
	if err = svc.Init(initCtx); err != nil {
		return errx.WithContext(err, "initialize service")
	}

	rpcv1.RegisterWorkerServiceServer(svr.GrpcServer, svc)

	go func() {
		err := periodicHeartbeat(ctx, svc, heartbeatInterval, heartbeatTimeout)
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
	if err := prepArguments(); err != nil {
		logx.Err(errx.WithContext(err, "prepare arguments"))
		os.Exit(1)
	}

	if err := run(); err != nil {
		logx.Err(err)
		os.Exit(1)
	}
}
