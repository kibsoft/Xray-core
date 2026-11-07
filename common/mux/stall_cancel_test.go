package mux

import (
	"context"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"testing"
	"time"
)

type stuckHandshake struct{ started, canceled chan struct{} }

func (s *stuckHandshake) Process(ctx context.Context, _ *transport.Link, _ internet.Dialer) error {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return ctx.Err()
}

func TestRetiringMuxWorkerCancelsStuckHandshake(t *testing.T) {
	p := &stuckHandshake{started: make(chan struct{}), canceled: make(chan struct{})}
	f := &DialingWorkerFactory{Proxy: p}
	w, err := f.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	select {
	case <-p.started:
	case <-time.After(time.Second):
		t.Fatal("transport not started")
	}
	w.Close()
	select {
	case <-p.canceled:
	case <-time.After(time.Second):
		t.Fatal("closed mux worker left handshake running")
	}
}
