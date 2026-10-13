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

func TestNewPlanCoversFileWithoutGaps(t *testing.T) {
	const total = int64(1000000)
	for _, conns := range []int{1, 3, 8, 64} {
		chunks := NewPlan(total, conns)
		if chunks[0].Start != 0 {
			t.Fatalf("conns=%d: first chunk starts at %d", conns, chunks[0].Start)
		}
		prev := int64(-1)
		for _, c := range chunks {
			if c.Start != prev+1 {
				t.Fatalf("conns=%d: gap before chunk %d (%d != %d)", conns, c.ID, c.Start, prev+1)
			}
			if c.Size() < MinChunkSize && len(chunks) > 1 {
				t.Fatalf("conns=%d: chunk %d smaller than minimum (%d)", conns, c.ID, c.Size())
			}
			prev = c.End
		}
		if prev != total-1 {
			t.Fatalf("conns=%d: last chunk ends at %d, want %d", conns, prev, total-1)
		}
	}
}

func TestSplitShrinksVictimAndKeepsCoverage(t *testing.T) {
	chunks := NewPlan(4_000_000, 1) // one big chunk
	nc, ok := Split(chunks, MinChunkSize)
	if !ok {
		t.Fatal("expected a split")
	}
	if nc.Start != chunks[0].End+1 {
		t.Fatalf("new chunk start %d does not follow victim end %d", nc.Start, chunks[0].End)
	}
	if nc.End != 4_000_000-1 {
		t.Fatalf("new chunk end %d", nc.End)
	}
	if done := chunks[0].Done; done != 0 {
		t.Fatalf("victim done changed to %d", done)
	}
	if _, ok := Split([]*Chunk{{ID: 0, Start: 0, End: MinChunkSize - 1}}, MinChunkSize); ok {
		t.Fatal("tiny chunk must not split")
	}
}

// rangeServer serves data honouring Range requests and counts them.
func rangeServer(t *testing.T, data []byte, hits *int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(hits, 1)
		w.Header().Set("Accept-Ranges", "bytes")
		rng := r.Header.Get("Range")
		if rng == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusOK)
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
		body := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
}

func TestSegmentedDownloadMultiConnection(t *testing.T) {
	data := make([]byte, 512*1024)
	for i := range data {
		data[i] = byte(i * 31)
	}
	var hits int64
	srv := rangeServer(t, data, &hits)
	defer srv.Close()

	dir := t.TempDir()
	d := New("t1", srv.URL+"/file.bin", dir, 4, nil, 0, srv.Client())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match the source")
	}
	if n := atomic.LoadInt64(&hits); n < 2 {
		t.Fatalf("expected multiple range requests, saw %d", n)
	}
	if !info.RangeOK {
		t.Fatal("expected range support to be detected")
	}
	if d.State().Single {
		t.Fatal("consistent ranges should keep using every connection")
	}
}

func TestDownloadWithoutRangeSupport(t *testing.T) {
	data := []byte(strings.Repeat("ochi-download-manager!", 4096))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Ignores Range entirely: always returns the whole body.
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("t2", srv.URL+"/plain.bin", dir, 8, nil, 0, srv.Client())
	d.Start(context.Background())

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if info.RangeOK {
		t.Fatal("range support should be false")
	}
	got, _ := os.ReadFile(filepath.Join(dir, info.FileName))
	if !bytes.Equal(got, data) {
		t.Fatal("content mismatch on non-range server")
	}
}

func TestResumeFromPersistedState(t *testing.T) {
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i)
	}
	var hits int64
	srv := rangeServer(t, data, &hits)
	defer srv.Close()

	dir := t.TempDir()
	const half = 128 * 1024

	// Simulate an earlier session: first chunk done, second not started.
	tmp := filepath.Join(dir, "resume.bin.odmpart")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(data[:half], 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(len(data))); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	st := State{
		ID: "t3", URL: srv.URL + "/resume.bin", FinalURL: srv.URL + "/resume.bin",
		FileName: "resume.bin", Dir: dir, Connections: 4, Total: int64(len(data)), RangeOK: true,
		Status: StatusPaused,
		Chunks: []*Chunk{
			{ID: 0, Start: 0, End: half - 1, Done: half},
			{ID: 1, Start: half, End: int64(len(data)) - 1, Done: 0},
		},
	}
	d := Restore(st, srv.Client())
	d.Start(context.Background())

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	got, err := os.ReadFile(filepath.Join(dir, "resume.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resumed file does not match the source")
	}
}
