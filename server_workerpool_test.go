package fastrpc

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aradilov/fastrpc/tlv"
)

// goroutineID is the id of the calling goroutine, from its stack header.
func goroutineID() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	id, _ := strconv.ParseUint(string(b), 10, 64)
	return id
}

// Without pipelining a request used to get a goroutine of its own. Now the
// requests run on pooled workers, so many requests are served by few
// goroutines.
func TestServerRunsRequestsOnPooledWorkers(t *testing.T) {
	var (
		mu  sync.Mutex
		ids = map[uint64]bool{}
	)

	s := &Server{
		NewHandlerCtx: newTestHandlerCtx,
		Handler: func(ctxv HandlerCtx) HandlerCtx {
			mu.Lock()
			ids[goroutineID()] = true
			mu.Unlock()
			return testEchoHandler(ctxv)
		},
		Logger:      &nilLogger{},
		Concurrency: 16,
	}

	serverStop, ln := newTestServerExt(s)
	c := newTestClient(ln)

	const requests = 500
	for i := range requests {
		var req tlv.Request
		var resp tlv.Response
		req.SwapValue([]byte("x"))
		if err := c.DoDeadline(&req, &resp, time.Now().Add(time.Second)); err != nil {
			t.Fatalf("request %d: %s", i, err)
		}
		if string(resp.Value()) != "x" {
			t.Fatalf("request %d: unexpected response %q", i, resp.Value())
		}
	}

	mu.Lock()
	served := len(ids)
	mu.Unlock()

	// sequential requests mostly land on the same idle worker; a handful at
	// most, never one per request
	if served > 16 {
		t.Fatalf("%d requests ran on %d goroutines; want them reused by the pool", requests, served)
	}

	if n := s.ConcurrencyCount(); n != 0 {
		t.Fatalf("ConcurrencyCount = %d after every request finished, want 0", n)
	}

	if err := serverStop(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerPoolSize(t *testing.T) {
	for _, c := range []struct{ in, want int }{
		{1, 1}, {2, 2}, {3, 4}, {16, 16}, {17, 32}, {32768, 32768}, {DefaultConcurrency, workerPoolSize(DefaultConcurrency)},
	} {
		got := workerPoolSize(c.in)
		if got != c.want || got&(got-1) != 0 || got < c.in {
			t.Errorf("workerPoolSize(%d) = %d, want %d (a power of two >= it)", c.in, got, c.want)
		}
	}
}

// deepHandler grows its stack well past a goroutine's starting size, as a real
// handler decoding and processing a request does.
func deepHandler(ctxv HandlerCtx) HandlerCtx {
	recurse(64)
	return testEchoHandler(ctxv)
}

//go:noinline
func recurse(n int) byte {
	var pad [256]byte
	pad[n%256] = byte(n)
	if n == 0 {
		return pad[0]
	}
	return recurse(n-1) + pad[n%256]
}

// BenchmarkServerDeepHandler measures requests whose handler needs a grown
// stack, sent concurrently over a few connections without pipelining.
func BenchmarkServerDeepHandler(b *testing.B) {
	s := &Server{
		NewHandlerCtx: newTestHandlerCtx,
		Handler:       deepHandler,
		Logger:        &nilLogger{},
		Concurrency:   1024,
	}

	serverStop, ln := newTestServerExt(s)
	defer serverStop()

	c := newTestClient(ln)
	c.MaxPendingRequests = 1024

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var req tlv.Request
		var resp tlv.Response
		for pb.Next() {
			req.SwapValue([]byte("x"))
			if err := c.DoDeadline(&req, &resp, time.Now().Add(5*time.Second)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
