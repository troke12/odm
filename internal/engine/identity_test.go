package engine

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestRangeRequestSendsIdentity(t *testing.T) {
	data := make([]byte, 128*1024)
	var mu sync.Mutex
	var encodings []string
	srv := encodingServer(t, data, true, &mu, &encodings)
	defer srv.Close()

	d := New("identity", srv.URL+"/file.bin", t.TempDir(), 2, nil, 0, srv.Client())
	d.Start(context.Background())
	if d.Info().Status != StatusComplete {
		t.Fatalf("status=%s err=%q", d.Info().Status, d.Info().Error)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(encodings) == 0 {
		t.Fatal("no requests")
	}
	for _, got := range encodings {
		if got != "identity" {
			t.Fatalf("Accept-Encoding = %q, want identity", got)
		}
	}
}

func TestRangeKeepsExplicitEncoding(t *testing.T) {
	data := []byte("explicit-encoding-bytes")
	var mu sync.Mutex
	var encodings []string
	srv := encodingServer(t, data, true, &mu, &encodings)
	defer srv.Close()

	d := New("keep", srv.URL+"/file.bin", t.TempDir(), 1, map[string]string{"Accept-Encoding": "gzip"}, 0, srv.Client())
	d.Start(context.Background())
	if d.Info().Status != StatusComplete {
		t.Fatalf("status=%s err=%q", d.Info().Status, d.Info().Error)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(encodings) == 0 {
		t.Fatal("no requests")
	}
	for _, got := range encodings {
		if got != "gzip" {
			t.Fatalf("Accept-Encoding = %q, want gzip", got)
		}
	}
}

func TestSingleStreamLeavesEncodingToTransport(t *testing.T) {
	var mu sync.Mutex
	var encodings []string
	srv := encodingServer(t, []byte("abcd"), false, &mu, &encodings)
	defer srv.Close()

	d := New("plain", srv.URL+"/a.bin", t.TempDir(), 4, nil, 0, srv.Client())
	d.Start(context.Background())
	info := d.Info()
	if info.Status != StatusComplete || info.RangeOK {
		t.Fatalf("status=%s range=%v err=%q", info.Status, info.RangeOK, info.Error)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(encodings) < 2 {
		t.Fatalf("requests = %d, want the probe and the transfer", len(encodings))
	}
	if got := encodings[len(encodings)-1]; got == "identity" || got == "" {
		t.Fatalf("transfer Accept-Encoding = %q", got)
	}
}

// encodingServer records Accept-Encoding. ranged answers byte ranges; otherwise
// it always returns the whole body with status 200.
func encodingServer(t *testing.T, data []byte, ranged bool, mu *sync.Mutex, encodings *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		*encodings = append(*encodings, r.Header.Get("Accept-Encoding"))
		mu.Unlock()
		if !ranged {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
			return
		}
		w.Header().Set("Accept-Ranges", "bytes")
		rng := r.Header.Get("Range")
		if rng == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
			return
		}
		spec := strings.TrimPrefix(rng, "bytes=")
		parts := strings.SplitN(spec, "-", 2)
		start, _ := strconv.Atoi(parts[0])
		end := len(data) - 1
		if len(parts) == 2 && parts[1] != "" {
			if v, err := strconv.Atoi(parts[1]); err == nil {
				end = v
			}
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		if start < 0 || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
}
