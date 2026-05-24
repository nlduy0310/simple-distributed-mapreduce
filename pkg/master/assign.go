package master

import (
	"context"
	"fmt"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/logx"
)

func (s *Service) RunAssignLoop(parent context.Context) error {
	for parent.Err() == nil {
		taskPath, err := s.pollTask(parent)
		if err != nil {
			return err
		}

		workerKey, err := s.pollWorker(parent)
		if err != nil {
			return err
		}

		go s.doMap(parent, workerKey, taskPath)
	}

	return nil
}

func (s *Service) doMap(ctx context.Context, workerName, taskPath string) {
	defer s.releaseWorker(workerName)

	found, err := s.reg.doMap(ctx, workerName, taskPath)
	if !found {
		logx.Warnf("received free worker key %s but was not found", workerName)
	} else if err != nil && (!errx.OneOf(err, context.Canceled, context.DeadlineExceeded) || ctx.Err() == nil) {
		logx.Warnf("a map task failed: %s", err.Error())
	}
}

func (s *Service) releaseWorker(workerKey string) {
	s.reg.releaseWorker(workerKey)
}

func (s *Service) pollTask(ctx context.Context) (string, error) {
	select {
	case taskKey := <-s.reg.pendingTasks:
		path, found := s.reg.taskPath(taskKey)
		if !found {
			return "", fmt.Errorf("task with key %s not found", taskKey)
		}
		return path, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *Service) pollWorker(ctx context.Context) (string, error) {
	select {
	case workerKey := <-s.reg.freeWorkers:
		return workerKey, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}
