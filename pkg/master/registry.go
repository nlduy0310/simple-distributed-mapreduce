package master

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	errWorkerNotFound = errors.New("worker not found")
	errWorkerExists   = errors.New("worker already exists")
)

type registry struct {
	wmu         sync.RWMutex
	workers     map[string]*worker // keyed by worker name
	freeWorkers chan string        // may contain removed workers

	tmu          sync.RWMutex
	tasks        map[string]*maptask // keyed by file path
	pendingTasks chan string
}

func newRegistry(paths []string, maxWorkers int) *registry {
	ret := &registry{
		workers:      make(map[string]*worker),
		freeWorkers:  make(chan string, maxWorkers),
		tasks:        make(map[string]*maptask, len(paths)),
		pendingTasks: make(chan string, len(paths)),
	}

	ret.initializeTasks(paths)

	return ret
}

func (r *registry) initializeTasks(paths []string) {
	for _, path := range paths {
		r.tasks[path] = newMaptask(path)
		r.pendingTasks <- path
	}
}

func (r *registry) hasWorker(name string) bool {
	r.wmu.RLock()
	defer r.wmu.RUnlock()

	_, ok := r.workers[name]
	return ok
}

func (r *registry) registerWorker(name, address string) error {
	if r.hasWorker(name) {
		return errWorkerExists
	}

	r.wmu.Lock()
	defer r.wmu.Unlock()

	w, err := newWorker(address)
	if err != nil {
		return err
	}

	r.workers[name] = w
	r.freeWorkers <- name
	return nil
}

func (r *registry) lastHeartbeat(name string) (time.Time, error) {
	r.wmu.RLock()
	defer r.wmu.RUnlock()

	w, ok := r.workers[name]
	if !ok {
		return time.Time{}, errWorkerNotFound
	}

	return w.lastHeartbeat(), nil
}

func (r *registry) recordHeartbeat(name string, at time.Time) error {
	r.wmu.RLock()
	defer r.wmu.RUnlock()

	w, ok := r.workers[name]
	if !ok {
		return errWorkerNotFound
	}

	w.recordHeartbeat(at)
	return nil
}

func (r *registry) removeWorker(name string) bool {
	r.wmu.Lock()
	defer r.wmu.Unlock()

	w, ok := r.workers[name]
	if !ok {
		return false
	}

	w.close()
	delete(r.workers, name)
	return true
}

func (r *registry) workerNames() []string {
	r.wmu.RLock()
	defer r.wmu.RUnlock()

	ret := make([]string, 0, len(r.workers))
	for name := range r.workers {
		ret = append(ret, name)
	}
	return ret
}

func (r *registry) taskPath(key string) (string, bool) {
	r.tmu.RLock()
	defer r.tmu.RUnlock()

	t, ok := r.tasks[key]
	if !ok {
		return "", false
	}

	return t.path(), true
}

// doMap returns if the worker was found and if any error occurred during rpc call
func (r *registry) doMap(ctx context.Context, name, path string) (bool, error) {
	r.wmu.RLock()
	defer r.wmu.RUnlock()

	w, ok := r.workers[name]
	if !ok {
		return false, nil
	}

	return true, w.doMap(ctx, path)
}

func (r *registry) releaseWorker(name string) {
	r.freeWorkers <- name
}
