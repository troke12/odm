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
	"sync/atomic"
	"testing"
	"time"
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

func TestSplitPrefersTheSlowerChunk(t *testing.T) {
	fast := &Chunk{ID: 0, Start: 0, End: 8<<20 - 1, pace: 1 << 20}        // 8 MiB at 1 MiB/s
	slow := &Chunk{ID: 1, Start: 8 << 20, End: 9<<20 - 1, pace: 16 << 10} // 1 MiB at 16 KiB/s
	fastEnd := fast.End
	nc, ok := Split([]*Chunk{fast, slow}, MinChunkSize)
	if !ok {
		t.Fatal("expected a split")
	}
	if fast.End != fastEnd {
		t.Fatalf("fast chunk was split, end %d", fast.End)
	}
	if nc.Start != slow.End+1 || slow.End >= 9<<20-1 {
		t.Fatalf("slow chunk end %d, new start %d", slow.End, nc.Start)
	}
	if nc.End != 9<<20-1 {
		t.Fatalf("new chunk end %d", nc.End)
	}
}

func TestSplitKeepsLargestWhenPacesMatch(t *testing.T) {
	// 5 MiB and 4 MiB at the same pace. The longer one is not twice as slow,
	// so the split stays on the larger remainder.
	big := &Chunk{ID: 0, Start: 0, End: 5<<20 - 1, pace: 1 << 20}
	small := &Chunk{ID: 1, Start: 5 << 20, End: 9<<20 - 1, pace: 1 << 20}
	smallEnd := small.End
	nc, ok := Split([]*Chunk{big, small}, MinChunkSize)
	if !ok {
		t.Fatal("expected a split")
	}
	if small.End != smallEnd {
		t.Fatalf("smaller chunk was split, end %d", small.End)
	}
	if nc.Start != big.End+1 {
		t.Fatalf("new chunk does not follow the larger chunk (%d, end %d)", nc.Start, big.End)
	}
}

func TestSplitKeepsLargestWhenOnlyALittleSlower(t *testing.T) {
	// 8 MiB at 2 MiB/s finishes in 4s. 3 MiB at 512 KiB/s finishes in 6s.
	// Six seconds is not twice four, so the larger remainder is still split.
	big := &Chunk{ID: 0, Start: 0, End: 8<<20 - 1, pace: 2 << 20}
	small := &Chunk{ID: 1, Start: 8 << 20, End: 11<<20 - 1, pace: 512 << 10}
	smallEnd := small.End
	nc, ok := Split([]*Chunk{big, small}, MinChunkSize)
	if !ok {
		t.Fatal("expected a split")
	}
	if small.End != smallEnd {
		t.Fatalf("slightly slower chunk was split, end %d", small.End)
	}
	if nc.Start != big.End+1 {
		t.Fatalf("new chunk does not follow the larger chunk (%d, end %d)", nc.Start, big.End)
	}
}

// A connection that went quiet keeps the pace it had, unless the pace
// decays with the time since its last bytes. It must be picked as the
// slowest even though it was fast and its remainder is the smaller one.
func TestSplitPicksAStalledChunk(t *testing.T) {
	now := time.Unix(1000, 0)
	busy := &Chunk{ID: 0, Start: 0, End: 8<<20 - 1, pace: 1 << 20,
		paceAt: now.Add(-50 * time.Millisecond), paceBytes: 50 << 10}
	quiet := &Chunk{ID: 1, Start: 8 << 20, End: 10<<20 - 1, pace: 4 << 20,
		paceAt: now.Add(-10 * time.Second)} // no byte for 10 s
	quietEnd := quiet.End
	nc, ok := splitAt([]*Chunk{busy, quiet}, MinChunkSize, now)
	if !ok {
		t.Fatal("expected a split")
	}
	if quiet.End == quietEnd || nc.Start != quiet.End+1 {
		t.Fatalf("the stalled chunk was not split (end %d, new start %d)", quiet.End, nc.Start)
	}
}

// A response that has not brought its first byte after paceStall is a
// stalled connection too, not an unknown one to skip.
func TestSplitPicksAChunkWithNoBytesYet(t *testing.T) {
	now := time.Unix(1000, 0)
	busy := &Chunk{ID: 0, Start: 0, End: 8<<20 - 1, pace: 1 << 20}
	waiting := &Chunk{ID: 1, Start: 8 << 20, End: 10<<20 - 1, fetchAt: now.Add(-2 * paceStall)}
	nc, ok := splitAt([]*Chunk{busy, waiting}, MinChunkSize, now)
	if !ok {
		t.Fatal("expected a split")
	}
	if nc.Start != waiting.End+1 {
		t.Fatalf("the chunk without bytes was not split (new start %d)", nc.Start)
	}
	// Before paceStall it is still unknown, and the largest remainder wins.
	busy2 := &Chunk{ID: 0, Start: 0, End: 8<<20 - 1, pace: 1 << 20}
	fresh := &Chunk{ID: 1, Start: 8 << 20, End: 10<<20 - 1, fetchAt: now.Add(-paceWindow)}
	nc, ok = splitAt([]*Chunk{busy2, fresh}, MinChunkSize, now)
	if !ok || nc.Start != busy2.End+1 {
		t.Fatal("a response just asked for was treated as stalled")
	}
}

// A new response starts a new measurement: the first window after a pause
// must not divide by the time the download was paused.
func TestStartPaceForgetsTheEarlierResponse(t *testing.T) {
	c := &Chunk{}
	t0 := time.Unix(0, 0)
	c.notePace(1000, t0)
	resume := t0.Add(10 * time.Second)
	c.startPace(resume)
	c.notePace(1000, resume)
	c.notePace(1000, resume.Add(paceWindow))
	if c.pace != 10000 {
		t.Fatalf("pace = %d, want 10000 (2000 bytes over one window)", c.pace)
	}
}

func TestNotePaceNeedsAWindow(t *testing.T) {
	c := &Chunk{}
	start := time.Unix(0, 0)
	c.notePace(1000, start)
	if c.pace != 0 {
		t.Fatalf("pace %d before the window", c.pace)
	}
	c.notePace(1000, start.Add(paceWindow))
	if c.pace != 10000 {
		t.Fatalf("pace = %d, want 10000", c.pace)
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
}

func TestStalledConnectionRetriesFromLastByte(t *testing.T) {
	data := make([]byte, 200*1024)
	for i := range data {
		data[i] = byte(i * 17)
	}
	var transfers atomic.Int32
	var mu sync.Mutex
	var ranges []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		spec := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		parts := strings.SplitN(spec, "-", 2)
		start, _ := strconv.Atoi(parts[0])
		end := len(data) - 1
		explicit := len(parts) == 2 && parts[1] != "" && !isProbe(r)
		if explicit {
			if v, err := strconv.Atoi(parts[1]); err == nil {
				end = v
			}
		}
		if end >= len(data) {
			end = len(data) - 1
		}
		if start < 0 || start > end {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		body := data[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(http.StatusPartialContent)
		// The size probe uses an open-ended range and does not read the body.
		if !explicit {
			return
		}
		mu.Lock()
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()
		if transfers.Add(1) == 1 {
			const first = 4096
			if _, err := w.Write(data[start : start+first]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("stall", srv.URL+"/stall.bin", dir, 1, nil, 0, srv.Client())
	d.stallTimeout = 200 * time.Millisecond
	d.Start(context.Background())

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if transfers.Load() < 2 {
		t.Fatalf("stalled connection was not retried, transfers=%d", transfers.Load())
	}
	// The retry continues after the 4096 bytes already written, rather
	// than starting the chunk over.
	mu.Lock()
	retry := ranges[1]
	mu.Unlock()
	if want := fmt.Sprintf("bytes=4096-%d", len(data)-1); retry != want {
		t.Fatalf("retry Range = %q, want %q", retry, want)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("bytes after a stalled connection do not match")
	}
}

func TestHeaderStallRetries(t *testing.T) {
	data := bytes.Repeat([]byte("h"), 128*1024)
	var transfers atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, explicit := testRange(r, len(data))
		if start < 0 || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if explicit && transfers.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		writeTestRange(w, data, start, end)
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("header-stall", srv.URL+"/h.bin", dir, 1, nil, 0, srv.Client())
	d.headerTimeout = 200 * time.Millisecond
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q transfers=%d", info.Status, info.Error, transfers.Load())
	}
	if transfers.Load() < 2 {
		t.Fatalf("header stall was not retried, transfers=%d", transfers.Load())
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("bytes after a header stall do not match")
	}
}

func TestPauseDuringHeaderWaitIsPaused(t *testing.T) {
	data := bytes.Repeat([]byte("p"), 128*1024)
	var d *Download
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, explicit := testRange(r, len(data))
		if start < 0 || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if explicit {
			d.Pause()
			<-release
			return
		}
		writeTestRange(w, data, start, end)
	}))
	defer srv.Close()
	defer close(release)

	dir := t.TempDir()
	d = New("header-pause", srv.URL+"/p.bin", dir, 1, nil, 0, srv.Client())
	d.headerTimeout = time.Second
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusPaused {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
}

// A server that takes longer than the stall limit to answer (building an
// archive or an export first) is waited for up to the header limit, not
// failed as a stalled connection.
func TestSlowHeadersWithinHeaderLimit(t *testing.T) {
	data := bytes.Repeat([]byte("s"), 128*1024)
	var transfers atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end, explicit := testRange(r, len(data))
		if start < 0 || end < start {
			http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if explicit {
			transfers.Add(1)
			time.Sleep(400 * time.Millisecond)
		}
		writeTestRange(w, data, start, end)
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("slow-headers", srv.URL+"/s.bin", dir, 1, nil, 0, srv.Client())
	d.stallTimeout = 100 * time.Millisecond
	d.headerTimeout = 2 * time.Second
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if n := transfers.Load(); n != 1 {
		t.Fatalf("transfers=%d, want 1: a slow answer is not a stall", n)
	}
}

// probeRange is what Inspect asks for. Test servers tell the probe from a
// transfer by it: a transfer always names both ends of its range.
const probeRange = "bytes=0-0"

func isProbe(r *http.Request) bool { return r.Header.Get("Range") == probeRange }

// testRange parses a request's Range. explicit is false for the probe and
// for an open-ended range, true for a transfer.
func testRange(r *http.Request, total int) (start, end int, explicit bool) {
	spec := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	start, _ = strconv.Atoi(parts[0])
	end = total - 1
	if len(parts) == 2 && parts[1] != "" {
		if v, err := strconv.Atoi(parts[1]); err == nil {
			end = v
		}
	}
	if end >= total {
		end = total - 1
	}
	return start, end, len(parts) == 2 && parts[1] != "" && !isProbe(r)
}

func writeTestRange(w http.ResponseWriter, data []byte, start, end int) {
	body := data[start : end+1]
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(body)
}

func closeEngineFile(d *Download) {
	d.mu.Lock()
	if d.file != nil {
		_ = d.file.Close()
		d.file = nil
	}
	d.mu.Unlock()
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
