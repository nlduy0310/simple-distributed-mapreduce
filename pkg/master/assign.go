package master

import (
	"context"
	"fmt"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/ctxx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/logx"
)

func (s *Service) RunAssignLoop(parent context.Context) error {
	for parent.Err() == nil {
		taskKey, err := s.pollTask(parent)
		if err != nil {
			return err
		}

		workerKey, err := s.pollWorker(parent)
		if err != nil {
			return err
		}

		go func() {
			err = s.doMap(parent, workerKey, taskKey)
			if err != nil && (!errx.IsContext(err) || !ctxx.Expired(parent)) {
				logx.Warnf("a map task failed: %s", err.Error())
			}
		}()
	}

	return nil
}

func (s *Service) doMap(parent context.Context, workerKey, taskKey string) (e error) {
	defer s.releaseWorker(workerKey)
	defer func() {
		if e != nil {
			s.reg.renewTask(taskKey)
		}
	}()

	path, found := s.reg.taskPath(taskKey)
	if !found {
		return fmt.Errorf("task with key %s not found", taskKey)
	}

	ctx, cancel := context.WithTimeout(parent, s.Config.MapTimeout)
	defer cancel()

	found, err := s.reg.doMap(ctx, workerKey, path)
	if !found {
		return fmt.Errorf("worker with key %s not found", workerKey)
	} else if err != nil {
		return err
	}

	return nil
}

func (s *Service) releaseWorker(workerKey string) {
	s.reg.releaseWorker(workerKey)
}

func (s *Service) pollTask(ctx context.Context) (string, error) {
	select {
	case taskKey := <-s.reg.pendingTasks:
		return taskKey, nil
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
