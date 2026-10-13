package engine

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentTransportDisablesHTTP2(t *testing.T) {
	base := &http.Transport{
		ForceAttemptHTTP2: true,
		MaxIdleConns:      64,
		Proxy: func(*http.Request) (*url.URL, error) {
			return url.Parse("http://proxy.example:8080")
		},
	}
	h1 := segmentTransport(base)
	if !base.ForceAttemptHTTP2 {
		t.Fatal("cloning changed the shared transport")
	}
	if h1.ForceAttemptHTTP2 {
		t.Fatal("segment transport still forces HTTP/2")
	}
	if h1.TLSNextProto == nil || len(h1.TLSNextProto) != 0 {
		t.Fatal("HTTP/2 is still enabled")
	}
	if h1.MaxIdleConns != 64 {
		t.Fatalf("idle conn pool = %d", h1.MaxIdleConns)
	}
	req, err := http.NewRequest(http.MethodGet, "https://files.example/a.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := h1.Proxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if got.String() != "http://proxy.example:8080" {
		t.Fatalf("proxy = %s", got)
	}
	if segmentTransport(nil).TLSNextProto == nil {
		t.Fatal("nil base left HTTP/2 on")
	}
}

func TestSegmentedDownloadUsesHTTP1(t *testing.T) {
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i)
	}
	var hits int64
	srv := rangeServer(t, data, &hits)
	defer srv.Close()

	dir := t.TempDir()
	d := New("h1", srv.URL+"/file.bin", dir, 4, nil, 0, srv.Client())
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
		t.Fatal("downloaded bytes do not match")
	}
	if d.h1 == nil {
		t.Fatal("several connections did not get an HTTP/1.1 client")
	}
	tr, ok := d.h1.Transport.(*http.Transport)
	if !ok || tr.ForceAttemptHTTP2 || tr.TLSNextProto == nil {
		t.Fatal("segment client can still negotiate HTTP/2")
	}
	if d.h1.Transport == d.client.Transport {
		t.Fatal("segment client shares the original transport")
	}
}

func TestOneConnectionKeepsSharedClient(t *testing.T) {
	data := []byte("one-connection-stays-on-the-shared-client")
	var hits int64
	srv := rangeServer(t, data, &hits)
	defer srv.Close()

	d := New("one", srv.URL+"/one.bin", t.TempDir(), 1, nil, 0, srv.Client())
	d.Start(context.Background())
	if d.Info().Status != StatusComplete {
		t.Fatalf("status=%s err=%q", d.Info().Status, d.Info().Error)
	}
	if d.h1 != nil {
		t.Fatal("a single connection built an HTTP/1.1 client")
	}
}

func TestNoRangeKeepsSharedClient(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("abcd"))
	}))
	defer ts.Close()

	d := New("plain", ts.URL+"/a.bin", t.TempDir(), 8, nil, 0, ts.Client())
	d.Start(context.Background())
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q", info.Status, info.Error)
	}
	if info.RangeOK {
		t.Fatal("range support should be false")
	}
	if d.h1 != nil {
		t.Fatal("a single stream built an HTTP/1.1 client")
	}
}
