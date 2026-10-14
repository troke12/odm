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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func explicitRange(r *http.Request) (start, end int, ok bool) {
	spec := r.Header.Get("Range")
	parts := strings.SplitN(strings.TrimPrefix(spec, "bytes="), "-", 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, 0, false
	}
	start, _ = strconv.Atoi(parts[0])
	end, _ = strconv.Atoi(parts[1])
	return start, end, true
}

func writeProbe(w http.ResponseWriter, total int) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", total-1, total))
	w.Header().Set("Content-Length", strconv.Itoa(total))
	w.WriteHeader(http.StatusPartialContent)
}

func TestConnectionsRampAfterFileBytes(t *testing.T) {
	const conns = 4
	data := make([]byte, conns*MinChunkSize)
	for i := range data {
		data[i] = byte(i*13 + 1)
	}
	var explicit atomic.Int32
	arrivals := make(chan struct{}, conns)
	allowPrefix := make(chan struct{})
	allowRest := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, ok := explicitRange(r)
		if !ok {
			writeProbe(w, len(data))
			return
		}
		if start < 0 || end >= len(data) || start > end {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		explicit.Add(1)
		arrivals <- struct{}{}
		select {
		case <-allowPrefix:
		case <-r.Context().Done():
			return
		}
		body := data[start : end+1]
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		const prefix = 4096
		n := prefix
		if n > len(body) {
			n = len(body)
		}
		if _, err := w.Write(body[:n]); err != nil {
			return
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
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
	t.Cleanup(func() {
		d.mu.Lock()
		if d.file != nil {
			_ = d.file.Close()
			d.file = nil
		}
		d.mu.Unlock()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		d.Start(ctx)
		close(done)
	}()

	select {
	case <-arrivals:
	case <-time.After(5 * time.Second):
		t.Fatal("no transfer started")
	}
	time.Sleep(150 * time.Millisecond)
	if got := explicit.Load(); got != 1 {
		t.Fatalf("opened %d connections before any file bytes", got)
	}
	close(allowPrefix)
	for i := 1; i < conns; i++ {
		select {
		case <-arrivals:
		case <-done:
			t.Fatalf("download ended after %d connections", explicit.Load())
		case <-time.After(5 * time.Second):
			t.Fatalf("opened %d connections, want %d", explicit.Load(), conns)
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
	if got := explicit.Load(); got != int32(conns) {
		t.Fatalf("transfer requests = %d, want %d", got, conns)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match")
	}
}

func TestRefusedResponseDoesNotOpenMoreConnections(t *testing.T) {
	const conns = 4
	data := make([]byte, conns*MinChunkSize)
	var explicit atomic.Int32
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := explicitRange(r); !ok {
			writeProbe(w, len(data))
			return
		}
		if explicit.Add(1) == 1 {
			started <- struct{}{}
		}
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("busy", srv.URL+"/busy.bin", dir, conns, nil, 0, srv.Client())
	t.Cleanup(func() {
		d.mu.Lock()
		if d.file != nil {
			_ = d.file.Close()
			d.file = nil
		}
		d.mu.Unlock()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		d.Start(ctx)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no transfer started")
	}
	// The refused request is retried after 300ms. A second connection would
	// have arrived with the first one.
	time.Sleep(150 * time.Millisecond)
	if got := explicit.Load(); got != 1 {
		t.Fatalf("opened %d connections after a refused response", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("download did not stop")
	}
}
