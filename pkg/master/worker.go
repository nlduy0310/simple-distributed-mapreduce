package master

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/client"
	"github.com/nlduy0310/simple-distributed-mapreduce/pkg/errx"
	rpcv1 "github.com/nlduy0310/simple-distributed-mapreduce/rpc/v1"
)

// worker is concurrency-safe and needs to be used by pointer
type worker struct {
	mu sync.RWMutex

	addr string

	heartbeat time.Time

	conn   *client.Client
	client rpcv1.WorkerServiceClient
}

func newWorker(addr string) (*worker, error) {
	conn, err := client.New(addr)
	if err != nil {
		return nil, errx.WithContext(err, "init client")
	}
	client := rpcv1.NewWorkerServiceClient(conn.Conn)

	return &worker{
		addr:      addr,
		heartbeat: time.Now(),
		conn:      conn,
		client:    client,
	}, nil
}

func (w *worker) lastHeartbeat() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.heartbeat
}

func (w *worker) recordHeartbeat(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.heartbeat = t
}

func (w *worker) doMap(ctx context.Context, path string) error {
	w.mu.RLock()
	defer w.mu.RUnlock()

	req := &rpcv1.MapRequest{NfsPath: path}
	resp, err := w.client.Map(ctx, req)
	if err != nil {
		return err
	} else if !resp.Ok {
		return errors.New(resp.Reason)
	}

	return nil
}

func (w *worker) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.conn.Close()
}
