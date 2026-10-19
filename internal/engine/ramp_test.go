package engine

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// These tests need no sleeps to show that a connection did not open: the
// only thing that opens one is file bytes arriving, so while a test holds
// every byte back, no further connection can start.

func writeProbe(w http.ResponseWriter, total int) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", total))
	w.Header().Set("Content-Length", "1")
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write([]byte{0})
}

func writePart(w http.ResponseWriter, data []byte, start, end int) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
}

func rampData(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i*13 + 1)
	}
	return data
}

func opened(d *Download) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.rampOpened
}

func TestConnectionsRampAfterFileBytes(t *testing.T) {
	const conns = 4
	data := rampData(conns * MinChunkSize)
	arrivals := make(chan struct{}, 64)
	allowPrefix := make(chan struct{})
	allowRest := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProbe(r) {
			writeProbe(w, len(data))
			return
		}
		start, end, _ := testRange(r, len(data))
		arrivals <- struct{}{}
		select {
		case <-allowPrefix:
		case <-r.Context().Done():
			return
		}
		writePart(w, data, start, end)
		body := data[start : end+1]
		n := min(4096, len(body))
		if _, err := w.Write(body[:n]); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		select {
		case <-allowRest:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(body[n:])
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("ramp", srv.URL+"/ramp.bin", dir, conns, nil, 0, srv.Client())
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { d.Start(ctx); close(done) }()

	select {
	case <-arrivals:
	case <-time.After(5 * time.Second):
		t.Fatal("no transfer started")
	}
	// No byte has been sent, so nothing can have opened a second one.
	if n := opened(d); n != 1 {
		t.Fatalf("opened %d connections before any file bytes", n)
	}
	close(allowPrefix)
	for i := 1; i < conns; i++ {
		select {
		case <-arrivals:
		case <-done:
			t.Fatalf("download ended after %d connections", opened(d))
		case <-time.After(5 * time.Second):
			t.Fatalf("opened %d connections, want %d", opened(d), conns)
		}
	}
	close(allowRest)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("download did not finish")
	}
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if n := opened(d); n != conns {
		t.Fatalf("connections opened = %d, want %d", n, conns)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match")
	}
}

// One connection that never gets a byte must not keep the rest closed
// while another connection is bringing bytes in.
func TestRampPassesAStalledConnection(t *testing.T) {
	const conns = 3
	data := rampData(conns * MinChunkSize)
	var transfers atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProbe(r) {
			writeProbe(w, len(data))
			return
		}
		start, end, _ := testRange(r, len(data))
		if transfers.Add(1) == 2 {
			// The second connection hangs before its headers.
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		writePart(w, data, start, end)
		body := data[start : end+1]
		// Bytes trickle until the test has seen the third connection, so
		// the first connection cannot finish and take more work itself.
		for len(body) > 0 {
			n := min(1024, len(body))
			if _, err := w.Write(body[:n]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			body = body[n:]
			select {
			case <-release:
			case <-time.After(5 * time.Millisecond):
			}
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("stall-ramp", srv.URL+"/s.bin", dir, conns, nil, 0, srv.Client())
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { d.Start(ctx); close(done) }()

	deadline := time.After(5 * time.Second)
	for opened(d) < conns {
		select {
		case <-deadline:
			t.Fatalf("a stalled connection kept the rest closed (opened %d of %d)", opened(d), conns)
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("download did not finish")
	}
	if info := d.Info(); info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
}

// A server that refuses every transfer sends no file bytes, so it is not
// asked on more connections while the first one retries.
func TestRefusedResponseDoesNotOpenMoreConnections(t *testing.T) {
	const conns = 4
	data := rampData(conns * MinChunkSize)
	var refused atomic.Int32
	enough := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProbe(r) {
			writeProbe(w, len(data))
			return
		}
		if refused.Add(1) == 3 {
			close(enough)
		}
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	d := New("busy", srv.URL+"/busy.bin", t.TempDir(), conns, nil, 0, srv.Client())
	d.retryBase = time.Millisecond
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { d.Start(ctx); close(done) }()

	select {
	case <-enough:
	case <-time.After(5 * time.Second):
		t.Fatal("the transfer was not retried")
	}
	if n := opened(d); n != 1 {
		t.Fatalf("opened %d connections to a server refusing every transfer", n)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not stop")
	}
}

// After an ignored range (#10's one-connection mode) a pass opens only one.
func TestRampRespectsOneConnectionMode(t *testing.T) {
	data := rampData(4 * MinChunkSize)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProbe(r) {
			writeProbe(w, len(data))
			return
		}
		start, end, _ := testRange(r, len(data))
		writePart(w, data, start, end)
		_, _ = w.Write(data[start : end+1])
	}))
	defer srv.Close()

	d := New("alone", srv.URL+"/a.bin", t.TempDir(), 4, nil, 0, srv.Client())
	d.alone = true
	t.Cleanup(func() { closeEngineFile(d) })
	d.Start(context.Background())
	if info := d.Info(); info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if n := opened(d); n != 1 {
		t.Fatalf("opened %d connections in one-connection mode", n)
	}
}
