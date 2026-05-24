package master

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/logx"
	rpcv1 "github.com/nlduy0310/simple-distributed-mapreduce/rpc/v1"
)

type Service struct {
	rpcv1.UnimplementedMasterServiceServer
	Config Config
	reg    *registry
}

func NewService(cfg Config) (*Service, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, errx.WithContext(err, "invalid config")
	}

	return &Service{Config: cfg, reg: newRegistry(cfg.InputFiles, cfg.MaxWorkers)}, nil
}

func (s *Service) Register(_ context.Context, req *rpcv1.RegisterRequest) (*rpcv1.RegisterResponse, error) {
	if err := s.reg.registerWorker(req.Name, req.Address); err != nil {
		logx.Err(errx.WithContext(err, fmt.Sprintf("register worker %q at %q", req.Name, req.Address)))
		return nil, err
	}

	log.Printf("successfully registered worker %q at %q", req.Name, req.Address)
	return &rpcv1.RegisterResponse{Ok: true}, nil
}

func (s *Service) Heartbeat(_ context.Context, req *rpcv1.HeartbeatRequest) (*rpcv1.HeartbeatResponse, error) {
	ts := time.Now()
	if err := s.reg.recordHeartbeat(req.Name, ts); err != nil {
		return nil, err
	}

	// log.Printf("received heartbeat from %q at %s", req.Name, ts)
	return &rpcv1.HeartbeatResponse{Ok: true}, nil
}

// PeriodicHealthcheck periodically check latest worker heartbeats and remove them if necessary
func (s *Service) PeriodicHealthcheck(ctx context.Context, interval, healthy time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := s.doHealthcheck(time.Now(), healthy)
			if err != nil {
				logx.Err(errx.WithContext(err, "healthcheck"))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) doHealthcheck(now time.Time, healthy time.Duration) error {
	names := s.reg.workerNames()

	for _, name := range names {
		lastHeartbeat, err := s.reg.lastHeartbeat(name)
		if err != nil {
			return err
		}

		diff := now.Sub(lastHeartbeat)
		if diff <= healthy {
			continue
		}

		// println("removing worker", name)
		s.reg.removeWorker(name)
	}

	return nil
}
