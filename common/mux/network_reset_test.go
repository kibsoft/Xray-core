package mux

import (
	"testing"

	"github.com/xtls/xray-core/common/signal/done"
)

func TestNetworkResetClosesOldWorkers(t *testing.T) {
	worker := &ClientWorker{done: done.New()}
	picker := &IncrementalWorkerPicker{workers: []*ClientWorker{worker}}
	picker.Reset()
	if !worker.Closed() || len(picker.workers) != 0 {
		t.Fatal("old mux sessions must be closed and removed from reuse")
	}
	picker.Reset()
}
