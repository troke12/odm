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

// shortServer answers Range requests. The probe (an open-ended range) is
// served whole. The first shortTries transfer responses contain only the
// first 1024 bytes, then the connection ends.
func shortServer(t *testing.T, data []byte, shortTries int64, xfers *int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, open := parseTestRange(r.Header.Get("Range"), len(data))
		if start < 0 || start >= len(data) || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		body := data[start : end+1]
		if !open {
			n := atomic.AddInt64(xfers, 1)
			if n <= shortTries && len(body) > 1024 {
				body = body[:1024]
			}
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
}

func parseTestRange(h string, total int) (start, end int, open bool) {
	spec := strings.TrimPrefix(h, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	start, _ = strconv.Atoi(parts[0])
	end = total - 1
	if len(parts) < 2 || parts[1] == "" {
		return start, end, true
	}
	end, _ = strconv.Atoi(parts[1])
	return start, end, false
}

func closePart(d *Download) {
	d.mu.Lock()
	if d.file != nil {
		_ = d.file.Close()
		d.file = nil
	}
	d.mu.Unlock()
}

func TestShortBodyRetriesFromLastByte(t *testing.T) {
	data := bytes.Repeat([]byte("ochi"), 32*1024) // 128 KiB, one chunk
	var xfers int64
	srv := shortServer(t, data, 1, &xfers)
	defer srv.Close()

	dir := t.TempDir()
	d := New("short-retry", srv.URL+"/file.bin", dir, 1, nil, 0, srv.Client())
	d.retryBase = time.Millisecond
	t.Cleanup(func() { closePart(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q transfers=%d", info.Status, info.Error, atomic.LoadInt64(&xfers))
	}
	if n := atomic.LoadInt64(&xfers); n < 2 {
		t.Fatalf("expected a retry after the short body, saw %d transfers", n)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match the source")
	}
}

func TestShortBodyStopsAfterMaxRetries(t *testing.T) {
	data := bytes.Repeat([]byte("ochi"), 32*1024)
	var xfers int64
	srv := shortServer(t, data, 1000, &xfers)
	defer srv.Close()

	dir := t.TempDir()
	d := New("short-give-up", srv.URL+"/file.bin", dir, 1, nil, 0, srv.Client())
	d.retryBase = time.Millisecond
	t.Cleanup(func() { closePart(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusError {
		t.Fatalf("status=%s err=%q transfers=%d", info.Status, info.Error, atomic.LoadInt64(&xfers))
	}
	if info.Error != errShortBody.Error() {
		t.Fatalf("error=%q", info.Error)
	}
	if n := atomic.LoadInt64(&xfers); n != MaxRetries {
		t.Fatalf("transfers=%d, want %d", n, MaxRetries)
	}
}
