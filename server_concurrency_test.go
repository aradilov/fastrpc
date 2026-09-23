package fastrpc

import (
	"io"
	"net"
	"testing"
	"time"
)

// A refused connection used to be dropped on the floor still open: the accept
// loop logged a line and moved on, so the descriptor leaked and the client was
// left holding a socket nobody would ever read. It now closes, and says so.
func TestServerClosesAndReportsRefusedConnections(t *testing.T) {
	rejected := make(chan net.Addr, 4)

	s := &Server{
		NewHandlerCtx:    newTestHandlerCtx,
		Handler:          testEchoHandler,
		Logger:           &nilLogger{},
		PipelineRequests: true,
		Concurrency:      1,
		ConnRejected: func(addr net.Addr, concurrency int) {
			if concurrency != 1 {
				t.Errorf("ConnRejected reported concurrency %d, want 1", concurrency)
			}

			rejected <- addr
		},
	}

	serverStop, ln := newTestServerExt(s)

	// The first connection takes the only slot and keeps it: the slot is held
	// for as long as the connection is served, and this one never speaks, so
	// the server sits in the handshake read.
	held, err := ln.Dial()
	if err != nil {
		t.Fatalf("cannot dial: %s", err)
	}

	defer held.Close()

	// The accept loop counts the slot on its own goroutine, so the second dial
	// has to wait for it rather than race it.
	for i := 0; s.ConcurrencyCount() == 0; i++ {
		if i > 1000 {
			t.Fatal("the first connection never took a slot")
		}

		time.Sleep(time.Millisecond)
	}

	refused, err := ln.Dial()
	if err != nil {
		t.Fatalf("cannot dial: %s", err)
	}

	defer refused.Close()

	select {
	case addr := <-rejected:
		if addr == nil {
			t.Error("ConnRejected was given no address")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ConnRejected was never called")
	}

	// Closed rather than merely unserved: the client finds out at once instead
	// of waiting out a timeout on a connection that will never answer.
	if err = refused.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("cannot set read deadline: %s", err)
	}

	var buf [1]byte

	if _, err = refused.Read(buf[:]); err != io.EOF {
		t.Errorf("read from a refused connection = %v, want %v", err, io.EOF)
	}

	// The refusal must not consume the slot it was refused for, or one burst
	// over the limit would shut the server down for good.
	if n := s.ConcurrencyCount(); n != 1 {
		t.Errorf("concurrency count after a refusal = %d, want 1", n)
	}

	if err = serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}

// The per-connection buffer used to be sized by Concurrency, so raising the
// connection limit cost memory quadratically. It has its own setting now, and
// a default that does not move with the limit.
func TestPendingResponsesPerConnIsNotTheConcurrencyLimit(t *testing.T) {
	if n := (&Server{}).pendingResponsesPerConn(); n != DefaultPendingResponsesPerConn {
		t.Errorf("default pendingResponsesPerConn = %d, want %d", n, DefaultPendingResponsesPerConn)
	}

	if n := (&Server{Concurrency: 65536}).pendingResponsesPerConn(); n != DefaultPendingResponsesPerConn {
		t.Errorf("pendingResponsesPerConn followed Concurrency: %d", n)
	}

	if n := (&Server{PendingResponsesPerConn: 7}).pendingResponsesPerConn(); n != 7 {
		t.Errorf("pendingResponsesPerConn = %d, want the 7 it was given", n)
	}
}

// Nothing is reported when the limit is not the one this hook describes: without
// pipelining the limit counts requests, and HandlerCtx.ConcurrencyLimitError is
// what answers them.
func TestConnRejectedIsNotCalledWithoutPipelining(t *testing.T) {
	s := &Server{
		NewHandlerCtx: newTestHandlerCtx,
		Handler:       testEchoHandler,
		Logger:        &nilLogger{},
		Concurrency:   1,
		ConnRejected: func(addr net.Addr, concurrency int) {
			t.Error("ConnRejected was called with PipelineRequests unset")
		},
	}

	serverStop, ln := newTestServerExt(s)

	for i := 0; i < 4; i++ {
		c, err := ln.Dial()
		if err != nil {
			t.Fatalf("cannot dial: %s", err)
		}

		defer c.Close()
	}

	time.Sleep(50 * time.Millisecond)

	if err := serverStop(); err != nil {
		t.Fatalf("cannot shutdown server: %s", err)
	}
}
