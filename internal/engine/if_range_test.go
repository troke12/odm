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

func TestRepresentationDiffers(t *testing.T) {
	same := &Probe{ETag: `"a"`, LastModified: "Mon, 1 Jan 2024 00:00:00 GMT", Total: 100, RangeOK: true}
	if representationDiffers(`"a"`, same.LastModified, 100, true, 10, same) {
		t.Fatal("identical probe reported as a different file")
	}
	if representationDiffers("", "", 100, true, 10, same) {
		t.Fatal("a download saved before validators existed must still resume")
	}
	if !representationDiffers(`"a"`, "", 100, true, 10, &Probe{ETag: `"b"`, Total: 100, RangeOK: true}) {
		t.Fatal("etag change was ignored")
	}
	if !representationDiffers("", same.LastModified, 100, true, 10, &Probe{LastModified: "Tue, 2 Jan 2024 00:00:00 GMT", Total: 100, RangeOK: true}) {
		t.Fatal("modification time change was ignored")
	}
	if !representationDiffers(`"a"`, "", 100, true, 10, &Probe{ETag: `"a"`, Total: 80, RangeOK: true}) {
		t.Fatal("size change was ignored")
	}
	if !representationDiffers("", "", 100, true, 10, &Probe{Total: 100, RangeOK: false}) {
		t.Fatal("losing range support with bytes on disk was ignored")
	}
}

func TestResumeSendsIfRangeAndKeepsProgress(t *testing.T) {
	data := payload(256 * 1024)
	const etag = `"same"`
	var sawIfRange atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Range") == etag {
			sawIfRange.Store(true)
		}
		writeRanged(w, r, data, etag, "Wed, 1 Jan 2025 00:00:00 GMT")
	}))
	defer srv.Close()

	dir := t.TempDir()
	const half = 128 * 1024
	if err := os.WriteFile(filepath.Join(dir, "keep.bin.odmpart"), append(append([]byte{}, data[:half]...), make([]byte, len(data)-half)...), 0o644); err != nil {
		t.Fatal(err)
	}
	st := State{
		ID: "keep", URL: srv.URL + "/keep.bin", FinalURL: srv.URL + "/keep.bin",
		FileName: "keep.bin", Dir: dir, Connections: 1, Total: int64(len(data)), RangeOK: true,
		ETag: etag, Status: StatusPaused,
		Chunks: []*Chunk{
			{ID: 0, Start: 0, End: int64(len(data)) - 1, Done: half},
		},
	}
	d := Restore(st, srv.Client())
	d.Start(context.Background())
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if !sawIfRange.Load() {
		t.Fatal("resume did not send If-Range")
	}
	got, err := os.ReadFile(filepath.Join(dir, "keep.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resumed file does not match")
	}
	if d.State().ETag != etag {
		t.Fatalf("saved etag = %q", d.State().ETag)
	}
}

func TestResumeRestartsWhenETagChanges(t *testing.T) {
	oldData := payload(192 * 1024)
	newData := make([]byte, len(oldData))
	for i := range newData {
		newData[i] = byte(255 - oldData[i])
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeRanged(w, r, newData, `"v2"`, "")
	}))
	defer srv.Close()

	dir := t.TempDir()
	const half = 96 * 1024
	partial := make([]byte, len(oldData))
	copy(partial, oldData[:half])
	if err := os.WriteFile(filepath.Join(dir, "changed.bin.odmpart"), partial, 0o644); err != nil {
		t.Fatal(err)
	}
	st := State{
		ID: "changed", URL: srv.URL + "/changed.bin", FinalURL: srv.URL + "/changed.bin",
		FileName: "changed.bin", Dir: dir, Connections: 1, Total: int64(len(oldData)), RangeOK: true,
		ETag: `"v1"`, Status: StatusPaused,
		Chunks: []*Chunk{
			{ID: 0, Start: 0, End: int64(len(oldData)) - 1, Done: half},
		},
	}
	d := Restore(st, srv.Client())
	d.Start(context.Background())
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	got, err := os.ReadFile(filepath.Join(dir, "changed.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, newData) {
		t.Fatal("file was not replaced after the etag changed")
	}
	if d.State().ETag != `"v2"` {
		t.Fatalf("etag after restart = %q", d.State().ETag)
	}
}

func TestIfRangeFailureRestartsMidTransfer(t *testing.T) {
	first := payload(128 * 1024)
	second := make([]byte, len(first))
	for i := range second {
		second[i] = first[len(first)-1-i]
	}
	var generation atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		explicit := strings.Contains(r.Header.Get("Range"), "-") && !strings.HasSuffix(r.Header.Get("Range"), "-")
		if explicit && r.Header.Get("If-Range") == `"v1"` {
			generation.Store(1)
			w.Header().Set("ETag", `"v2"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(second)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(second)
			return
		}
		data, etag := first, `"v1"`
		if generation.Load() > 0 {
			data, etag = second, `"v2"`
		}
		writeRanged(w, r, data, etag, "")
	}))
	defer srv.Close()

	dir := t.TempDir()
	d := New("flip", srv.URL+"/flip.bin", dir, 1, nil, 0, srv.Client())
	d.Start(context.Background())
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, second) {
		t.Fatal("download kept the first representation after If-Range failed")
	}
}

func writeRanged(w http.ResponseWriter, r *http.Request, data []byte, etag, modified string) {
	w.Header().Set("Accept-Ranges", "bytes")
	if etag != "" {
		w.Header().Set("ETag", etag)
	}
	if modified != "" {
		w.Header().Set("Last-Modified", modified)
	}
	spec := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	start := 0
	if parts[0] != "" {
		start, _ = strconv.Atoi(parts[0])
	}
	end := len(data) - 1
	if len(parts) == 2 && parts[1] != "" {
		if v, err := strconv.Atoi(parts[1]); err == nil {
			end = v
		}
	}
	if start < 0 || start >= len(data) || end < start {
		http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if end >= len(data) {
		end = len(data) - 1
	}
	body := data[start : end+1]
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(body)
}
