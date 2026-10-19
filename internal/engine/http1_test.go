package engine

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
	// Cloning set up HTTP/2 on base; the copy must not offer it in ALPN.
	if p := h1.TLSClientConfig.NextProtos; len(p) != 1 || p[0] != "http/1.1" {
		t.Fatalf("segment transport ALPN = %v, want [http/1.1]", p)
	}
	if p := segmentTransport(nil).TLSClientConfig.NextProtos; len(p) != 1 || p[0] != "http/1.1" {
		t.Fatalf("nil base ALPN = %v, want [http/1.1]", p)
	}
}

// Over TLS with a server that speaks HTTP/2 (most CDNs), the segmented
// connections must negotiate HTTP/1.1 and each get a socket. Plain-HTTP
// tests never reach ALPN, which is where offering h2 broke every chunk.
func TestSegmentedDownloadOverHTTP2Server(t *testing.T) {
	data := make([]byte, 512*1024)
	for i := range data {
		data[i] = byte(i * 13)
	}
	var mu sync.Mutex
	protos := map[string]int{}
	conns := map[string]bool{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		protos[r.Proto]++
		if r.Header.Get("Range") != probeRange {
			conns[r.RemoteAddr] = true
		}
		mu.Unlock()
		http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(data))
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := srv.Client()
	if tr, ok := client.Transport.(*http.Transport); !ok || !tr.ForceAttemptHTTP2 {
		t.Fatal("test client is not set up for HTTP/2")
	}
	dir := t.TempDir()
	d := New("h2", srv.URL+"/f.bin", dir, 4, nil, 0, client)
	d.retryBase = time.Millisecond
	t.Cleanup(func() { closeEngineFile(d) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.Start(ctx)

	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status=%s err=%q protos=%v", info.Status, info.Error, protos)
	}
	got, err := os.ReadFile(filepath.Join(dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes do not match")
	}
	mu.Lock()
	defer mu.Unlock()
	if protos["HTTP/2.0"] < 1 {
		t.Fatalf("the probe did not use HTTP/2 (protos=%v); the test server is not speaking it", protos)
	}
	if protos["HTTP/1.1"] < 2 {
		t.Fatalf("segmented requests did not use HTTP/1.1: %v", protos)
	}
	if len(conns) < 2 {
		t.Fatalf("segmented requests shared %d socket(s)", len(conns))
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
	// The HTTP/1.1 client goes through the local-network guard too.
	g, ok := d.h1.Transport.(*localGuard)
	if !ok {
		t.Fatalf("segment client transport is %T, not the local-network guard", d.h1.Transport)
	}
	tr, ok := g.next.(*http.Transport)
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
