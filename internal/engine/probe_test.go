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
)

// TestProbeFirstByteStillSegments covers a server that answers 200 when the
// requested range is the whole file, and 206 for a narrower range. The probe
// must ask for the first byte so the download stays segmented.
func TestProbeFirstByteStillSegments(t *testing.T) {
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i * 3)
	}
	var oneByte int64
	var segments int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rng := r.Header.Get("Range")
		if rng == "bytes=0-0" {
			atomic.StoreInt64(&oneByte, 1)
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(data)))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[:1])
			return
		}
		start, end, whole := wholeFileRange(rng, len(data))
		if whole {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
			return
		}
		atomic.AddInt64(&segments, 1)
		if end >= len(data) {
			end = len(data) - 1
		}
		body := data[start : end+1]
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("probe-byte", srv.URL+"/file.bin", dir, 4, nil, 0, srv.Client())
	t.Cleanup(func() {
		d.mu.Lock()
		if d.file != nil {
			_ = d.file.Close()
			d.file = nil
		}
		d.mu.Unlock()
	})
	d.Start(context.Background())

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if !info.RangeOK {
		t.Fatal("range support was not detected")
	}
	if atomic.LoadInt64(&oneByte) != 1 {
		t.Fatal("probe did not request the first byte")
	}
	if n := atomic.LoadInt64(&segments); n < 2 {
		t.Fatalf("expected a segmented transfer, saw %d range requests", n)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match the source")
	}
}

// wholeFileRange reports the requested span. An empty range, an open end, or
// a span that covers every byte counts as the whole file.
func wholeFileRange(h string, total int) (start, end int, whole bool) {
	if h == "" {
		return 0, total - 1, true
	}
	spec := strings.TrimPrefix(h, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) == 0 || parts[0] == "" {
		return 0, total - 1, true
	}
	start, _ = strconv.Atoi(parts[0])
	if len(parts) < 2 || parts[1] == "" {
		return start, total - 1, true
	}
	end, _ = strconv.Atoi(parts[1])
	return start, end, start == 0 && end >= total-1
}
