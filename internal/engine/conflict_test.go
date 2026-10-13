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
	"sync"
	"testing"
)

func TestRewindKeepsBytesBeforeConflict(t *testing.T) {
	d := &Download{Chunks: []*Chunk{
		{ID: 0, Start: 0, End: 49, Done: 50},
		{ID: 1, Start: 50, End: 149, Done: 100, claimed: true},
		{ID: 2, Start: 150, End: 249, Done: 40, claimed: true},
	}}
	d.rewindLocked(90)
	if d.Chunks[0].Done != 50 {
		t.Fatalf("earlier chunk done = %d, want 50", d.Chunks[0].Done)
	}
	if d.Chunks[1].Done != 40 || d.Chunks[1].claimed {
		t.Fatalf("conflict chunk done=%d claimed=%v", d.Chunks[1].Done, d.Chunks[1].claimed)
	}
	if d.Chunks[2].Done != 0 || d.Chunks[2].claimed {
		t.Fatalf("later chunk done=%d claimed=%v", d.Chunks[2].Done, d.Chunks[2].claimed)
	}
}

func TestWitnessMismatchReportsOffset(t *testing.T) {
	d := &Download{witnesses: []witness{{off: 10, data: []byte{1, 2, 3}}}}
	if _, bad := d.mismatchLocked(8, []byte{9, 9, 1, 2, 3}); bad {
		t.Fatal("matching bytes reported a conflict")
	}
	at, bad := d.mismatchLocked(8, []byte{9, 9, 1, 0, 3})
	if !bad || at != 11 {
		t.Fatalf("conflict at %d, bad=%v, want offset 11", at, bad)
	}
}

func TestSingleFlagRestores(t *testing.T) {
	d := New("id", "http://example/a.bin", t.TempDir(), 4, nil, 0, nil)
	d.single = true
	again := Restore(d.State(), nil)
	if !again.single {
		t.Fatal("single was not restored")
	}
}

// rangeStats counts how the test server was called.
type rangeStats struct {
	mu          sync.Mutex
	inflight    int
	maxInflight int
	boundedZero int
}

func (s *rangeStats) enter() {
	s.mu.Lock()
	s.inflight++
	if s.inflight > s.maxInflight {
		s.maxInflight = s.inflight
	}
	s.mu.Unlock()
}

func (s *rangeStats) leave() {
	s.mu.Lock()
	s.inflight--
	s.mu.Unlock()
}

func (s *rangeStats) noteZero() {
	s.mu.Lock()
	s.boundedZero++
	s.mu.Unlock()
}

func (s *rangeStats) zeros() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boundedZero
}

func (s *rangeStats) peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInflight
}

// serveFile answers Range requests. corrupt reports whether the first body
// byte should be flipped. bounded is false for an open-ended range.
func serveFile(t *testing.T, data []byte, stats *rangeStats, corrupt func(start int, bounded bool) bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stats != nil {
			stats.enter()
			defer stats.leave()
		}
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
		bounded := len(parts) == 2 && parts[1] != ""
		if bounded {
			if v, err := strconv.Atoi(parts[1]); err == nil {
				end = v
			}
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		if start < 0 || start >= len(data) || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if stats != nil && bounded && start == 0 {
			stats.noteZero()
		}
		body := append([]byte(nil), data[start:end+1]...)
		if corrupt != nil && len(body) > 0 && corrupt(start, bounded) {
			body[0] ^= 0xff
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body)
	}))
}

func conflictData() []byte {
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i * 31)
	}
	return data
}

func TestConflictFallsBackToOneConnection(t *testing.T) {
	data := conflictData()
	var stats rangeStats
	var once sync.Once
	srv := serveFile(t, data, &stats, func(start int, _ bool) bool {
		if start <= 0 {
			return false
		}
		flip := false
		once.Do(func() { flip = true })
		return flip
	})
	defer srv.Close()

	dir := t.TempDir()
	d := New("conflict-once", srv.URL+"/file.bin", dir, 4, nil, 0, srv.Client())
	d.Start(context.Background())

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file does not match after falling back to one connection")
	}
	if !d.State().Single {
		t.Fatal("fallback did not stay on one connection")
	}
	if stats.peak() < 2 {
		t.Fatalf("peak connections = %d, want at least 2", stats.peak())
	}
	if stats.zeros() != 1 {
		t.Fatalf("requests from byte 0 = %d, want 1", stats.zeros())
	}
}

func TestConflictRestartsFromZeroThenMatches(t *testing.T) {
	data := conflictData()
	var stats rangeStats
	srv := serveFile(t, data, &stats, func(start int, _ bool) bool {
		// The first chunk of the opening pass and of the from-zero pass both
		// request byte 0. Anything earlier than the second of those is wrong.
		return start > 0 && stats.zeros() < 2
	})
	defer srv.Close()

	dir := t.TempDir()
	d := New("conflict-restart", srv.URL+"/file.bin", dir, 4, nil, 0, srv.Client())
	d.Start(context.Background())

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file does not match after restarting from the first byte")
	}
	if !d.State().Single {
		t.Fatal("restart did not stay on one connection")
	}
	if stats.zeros() < 2 {
		t.Fatalf("requests from byte 0 = %d, want at least 2", stats.zeros())
	}
}

func TestConflictStillFailsAfterRestart(t *testing.T) {
	data := conflictData()
	var stats rangeStats
	srv := serveFile(t, data, &stats, func(start int, _ bool) bool {
		return start > 0
	})
	defer srv.Close()

	dir := t.TempDir()
	d := New("conflict-fail", srv.URL+"/file.bin", dir, 4, nil, 0, srv.Client())
	d.Start(context.Background())

	info := d.Info()
	if info.Status != StatusError {
		t.Fatalf("status=%s, want error", info.Status)
	}
	if !strings.Contains(info.Error, "restarted from the first byte") {
		t.Fatalf("error = %q", info.Error)
	}
	if stats.zeros() < 2 {
		t.Fatalf("requests from byte 0 = %d, want the restart to run", stats.zeros())
	}
	if _, err := os.Stat(filepath.Join(dir, "file.bin")); !os.IsNotExist(err) {
		t.Fatalf("finished file exists, stat err=%v", err)
	}
	d.mu.Lock()
	if d.file != nil {
		_ = d.file.Close()
		d.file = nil
	}
	d.mu.Unlock()
}
