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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shortServer answers Range requests. The probe (an open-ended range) is
// served whole. A transfer response is cut by limit(n), n counting the
// transfers from 1: it returns how many bytes of the asked range to send,
// or -1 for all of them. The cut is clean: Content-Length matches what is
// sent, so the client sees io.EOF, not io.ErrUnexpectedEOF.
func shortServer(t *testing.T, data []byte, limit func(n int64) int, xfers *int64, ranges *[]string, mu *sync.Mutex) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, explicit := testRange(r, len(data))
		if start < 0 || start >= len(data) || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body := data[start : end+1]
		if explicit {
			n := atomic.AddInt64(xfers, 1)
			if ranges != nil {
				mu.Lock()
				*ranges = append(*ranges, r.Header.Get("Range"))
				mu.Unlock()
			}
			if l := limit(n); l >= 0 && l < len(body) {
				body = body[:l]
			}
		}
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
}

func runShort(t *testing.T, name string, srv *httptest.Server) (*Download, string) {
	t.Helper()
	dir := t.TempDir()
	d := New(name, srv.URL+"/file.bin", dir, 1, nil, 0, srv.Client())
	d.retryBase = time.Millisecond
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)
	return d, dir
}

func TestShortBodyRetriesFromLastByte(t *testing.T) {
	data := bytes.Repeat([]byte("ochi"), 32*1024) // 128 KiB, one chunk
	var xfers int64
	var mu sync.Mutex
	var ranges []string
	first := func(n int64) int {
		if n == 1 {
			return 1024
		}
		return -1
	}
	srv := shortServer(t, data, first, &xfers, &ranges, &mu)
	defer srv.Close()

	d, dir := runShort(t, "short-retry", srv)
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q transfers=%d", info.Status, info.Error, atomic.LoadInt64(&xfers))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ranges) < 2 {
		t.Fatalf("expected a second request after the short body, saw %d", len(ranges))
	}
	if want := fmt.Sprintf("bytes=1024-%d", len(data)-1); ranges[1] != want {
		t.Fatalf("second Range = %q, want %q", ranges[1], want)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match the source")
	}
}

// A server that caps every 206 makes progress on each response, so the
// chunk finishes however many responses it takes, beyond MaxRetries.
func TestCappedResponsesFinish(t *testing.T) {
	data := bytes.Repeat([]byte("cap!"), 32*1024) // 128 KiB: 128 responses of 1 KiB
	var xfers int64
	srv := shortServer(t, data, func(int64) int { return 1024 }, &xfers, nil, nil)
	defer srv.Close()

	d, dir := runShort(t, "capped", srv)
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q transfers=%d", info.Status, info.Error, atomic.LoadInt64(&xfers))
	}
	if n := atomic.LoadInt64(&xfers); n <= MaxRetries {
		t.Fatalf("transfers=%d, want more than MaxRetries for this test to mean anything", n)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match the source")
	}
}

// A 206 with an empty body makes no progress: it is retried with backoff
// and fails after MaxRetries, instead of being asked again at once forever.
func TestEmptyShortBodyStopsAfterMaxRetries(t *testing.T) {
	data := bytes.Repeat([]byte("ochi"), 32*1024)
	var xfers int64
	srv := shortServer(t, data, func(int64) int { return 0 }, &xfers, nil, nil)
	defer srv.Close()

	d, _ := runShort(t, "short-give-up", srv)
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
