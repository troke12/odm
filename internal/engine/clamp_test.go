package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestOwnedSpan(t *testing.T) {
	if got := ownedSpan(0, 100, 49); got != 50 {
		t.Fatalf("span past the end = %d", got)
	}
	if got := ownedSpan(40, 20, 49); got != 10 {
		t.Fatalf("span crossing the end = %d", got)
	}
	if got := ownedSpan(50, 10, 49); got != 0 {
		t.Fatalf("span already past the end = %d", got)
	}
	if got := ownedSpan(0, 10, -1); got != 10 {
		t.Fatalf("open end = %d", got)
	}
	if got := ownedSpan(0, 0, 100); got != 0 {
		t.Fatalf("empty read = %d", got)
	}
}

func TestReadAcrossAMovedEndIsNotCountedTwice(t *testing.T) {
	const total = 256 * 1024
	const cut = 1024
	data := bytes.Repeat([]byte{7}, total)
	client := &http.Client{Transport: &fullReadTrip{data: data}}

	dir := t.TempDir()
	d := New("clamp", "http://example.com/file.bin", dir, 1, nil, 0, client)
	var once sync.Once
	d.beforeWrite = func() {
		once.Do(func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if len(d.Chunks) != 1 {
				return
			}
			ch := d.Chunks[0]
			if ch.End < cut || ch.Done != 0 {
				return
			}
			d.Chunks = append(d.Chunks, &Chunk{ID: ch.ID + 1, Start: cut, End: ch.End})
			ch.End = cut - 1
		})
	}
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
	if got := d.doneBytes(); got != total {
		t.Fatalf("counted %d bytes, file is %d", got, total)
	}
	d.mu.Lock()
	first := d.Chunks[0]
	d.mu.Unlock()
	if first.End != cut-1 {
		t.Fatalf("first chunk end %d, want %d", first.End, cut-1)
	}
	if first.Done != first.Size() {
		t.Fatalf("first chunk counted %d bytes of its %d", first.Done, first.Size())
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match the source")
	}
}

// fullReadTrip answers ranges from memory. Each body read fills the caller's
// buffer, so one read can cross a chunk end that moves before the write.
type fullReadTrip struct {
	data []byte
}

func (rt *fullReadTrip) RoundTrip(req *http.Request) (*http.Response, error) {
	start, end := 0, len(rt.data)-1
	spec := strings.TrimPrefix(req.Header.Get("Range"), "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if parts[0] != "" {
		start, _ = strconv.Atoi(parts[0])
	}
	if len(parts) == 2 && parts[1] != "" {
		end, _ = strconv.Atoi(parts[1])
	}
	if start < 0 || start >= len(rt.data) || end < start {
		return &http.Response{
			StatusCode: http.StatusRequestedRangeNotSatisfiable,
			Status:     "416 Range Not Satisfiable",
			Proto:      "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header:  make(http.Header),
			Body:    io.NopCloser(strings.NewReader("")),
			Request: req,
		}, nil
	}
	if end >= len(rt.data) {
		end = len(rt.data) - 1
	}
	body := rt.data[start : end+1]
	header := make(http.Header)
	header.Set("Accept-Ranges", "bytes")
	header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(rt.data)))
	header.Set("Content-Length", strconv.Itoa(len(body)))
	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		Status:        "206 Partial Content",
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Body:          io.NopCloser(&fillBody{buf: append([]byte(nil), body...)}),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

type fillBody struct {
	buf []byte
	off int
}

func (b *fillBody) Read(p []byte) (int, error) {
	if b.off >= len(b.buf) {
		return 0, io.EOF
	}
	n := copy(p, b.buf[b.off:])
	b.off += n
	return n, nil
}
