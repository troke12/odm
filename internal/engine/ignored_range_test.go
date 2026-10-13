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
	"time"
)

func TestWholeFileRange(t *testing.T) {
	cases := []struct {
		start, end, total int64
		want              bool
	}{
		{0, 99, 100, true},
		{0, -1, 100, true},
		{0, -1, 0, true},
		{0, 50, 100, false},
		{10, 99, 100, false},
		{0, 99, 0, false},
	}
	for _, tc := range cases {
		if got := wholeFileRange(tc.start, tc.end, tc.total); got != tc.want {
			t.Errorf("wholeFileRange(%d, %d, %d) = %v, want %v", tc.start, tc.end, tc.total, got, tc.want)
		}
	}
}

func TestContiguousDoneStopsAtTheFirstHole(t *testing.T) {
	chunks := []*Chunk{
		{Start: 0, End: 99, Done: 100},
		{Start: 100, End: 199, Done: 40},
		{Start: 200, End: 299, Done: 100},
	}
	if got := contiguousDone(chunks, 300); got != 140 {
		t.Fatalf("prefix = %d, want 140", got)
	}
	hole := []*Chunk{
		{Start: 0, End: 99, Done: 0},
		{Start: 100, End: 199, Done: 100},
	}
	if got := contiguousDone(hole, 200); got != 0 {
		t.Fatalf("prefix with a hole at the start = %d", got)
	}
}

func TestPlanAloneKeepsContiguousPrefix(t *testing.T) {
	d := New("plan", "http://example/p.bin", t.TempDir(), 4, nil, 0, nil)
	d.Total = 300
	d.Chunks = []*Chunk{
		{ID: 0, Start: 0, End: 99, Done: 100},
		{ID: 1, Start: 100, End: 199, Done: 40},
		{ID: 2, Start: 200, End: 299, Done: 100},
	}
	d.planAlone()
	if len(d.Chunks) != 1 {
		t.Fatalf("chunks = %d", len(d.Chunks))
	}
	c := d.Chunks[0]
	if c.Start != 0 || c.End != 299 || c.Done != 140 {
		t.Fatalf("chunk = %+v, want start 0 end 299 done 140", c)
	}
}

func TestAloneSurvivesRestore(t *testing.T) {
	d := New("alone", "http://example/a.bin", t.TempDir(), 4, nil, 0, nil)
	d.alone = true
	d.Total = 10
	d.RangeOK = true
	d.Chunks = []*Chunk{{ID: 0, Start: 0, End: 9, Done: 4}}
	st := d.State()
	if !st.Alone {
		t.Fatal("alone was not saved")
	}
	got := Restore(st, nil)
	if !got.alone {
		t.Fatal("alone was not restored")
	}
}

// TestIgnoredSubrangeKeepsFinishedBytes resumes with a stored prefix. Every
// smaller range is answered with the whole file. The prefix stays, and the
// finished file matches.
func TestIgnoredSubrangeKeepsFinishedBytes(t *testing.T) {
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i*3 + 1)
	}
	const gap = 128 * 1024
	var mu sync.Mutex
	var ranges []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spec := r.Header.Get("Range")
		mu.Lock()
		ranges = append(ranges, spec)
		mu.Unlock()
		start := 0
		if spec != "" {
			parts := strings.SplitN(strings.TrimPrefix(spec, "bytes="), "-", 2)
			start, _ = strconv.Atoi(parts[0])
		}
		if start == 0 {
			http.Error(w, "restarted from byte 0", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	dir := t.TempDir()
	tmp := filepath.Join(dir, "kept.bin.odmpart")
	if err := os.WriteFile(tmp, data[:gap], 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(int64(len(data))); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	const part = 64 * 1024
	st := State{
		ID: "kept", URL: srv.URL + "/kept.bin", FinalURL: srv.URL + "/kept.bin",
		FileName: "kept.bin", Dir: dir, Connections: 4, Total: int64(len(data)), RangeOK: true,
		Status: StatusPaused,
		Chunks: []*Chunk{
			{ID: 0, Start: 0, End: part - 1, Done: part},
			{ID: 1, Start: part, End: 2*part - 1, Done: part},
			{ID: 2, Start: 2 * part, End: 3*part - 1, Done: 0},
			{ID: 3, Start: 3 * part, End: int64(len(data)) - 1, Done: 0},
		},
	}
	d := Restore(st, srv.Client())
	closeFile := func() {
		d.mu.Lock()
		if d.file != nil {
			_ = d.file.Close()
			d.file = nil
		}
		d.mu.Unlock()
	}
	t.Cleanup(closeFile)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if !d.alone {
		t.Fatal("download kept using multiple connections")
	}
	got, err := os.ReadFile(filepath.Join(dir, "kept.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file does not match after the whole-file response")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(ranges) == 0 {
		t.Fatal("no range was requested")
	}
	for _, spec := range ranges {
		parts := strings.SplitN(strings.TrimPrefix(spec, "bytes="), "-", 2)
		start, _ := strconv.Atoi(parts[0])
		if start < gap {
			t.Fatalf("request %s starts before the stored prefix %d", spec, gap)
		}
	}
}

// TestSubrangeOKFallsBackToOneConnection starts a segmented download. The
// probe advertises ranges, and every explicit range is answered with the
// whole file. The file is fetched on one connection and matches.
func TestSubrangeOKFallsBackToOneConnection(t *testing.T) {
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i*5 + 7)
	}
	var mu sync.Mutex
	var ranges []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spec := r.Header.Get("Range")
		mu.Lock()
		ranges = append(ranges, spec)
		mu.Unlock()
		parts := strings.SplitN(strings.TrimPrefix(spec, "bytes="), "-", 2)
		open := len(parts) == 2 && parts[1] == ""
		if open {
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("fall", srv.URL+"/fall.bin", dir, 4, nil, 0, srv.Client())
	t.Cleanup(func() {
		d.mu.Lock()
		if d.file != nil {
			_ = d.file.Close()
			d.file = nil
		}
		d.mu.Unlock()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if !info.RangeOK {
		t.Fatal("probe did not report range support")
	}
	if !d.alone {
		t.Fatal("download kept using multiple connections")
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("file does not match")
	}
	whole := fmt.Sprintf("bytes=0-%d", len(data)-1)
	var sawSubrange, sawWhole bool
	mu.Lock()
	defer mu.Unlock()
	for _, spec := range ranges {
		if spec == whole {
			sawWhole = true
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(spec, "bytes="), "-", 2)
		if len(parts) == 2 && parts[1] != "" {
			sawSubrange = true
		}
	}
	if !sawSubrange {
		t.Fatal("no smaller range was requested")
	}
	if !sawWhole {
		t.Fatalf("the remainder was not requested as %s; ranges %v", whole, ranges)
	}
}
