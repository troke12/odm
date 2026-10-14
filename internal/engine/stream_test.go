package engine

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"odm/internal/media"
)

func newStreamDownload(t *testing.T, client *http.Client, conns int, job StreamJob) *Download {
	t.Helper()
	d := New("s1", job.Manifest, t.TempDir(), conns, nil, 0, client)
	d.FileName = "clip"
	d.Stream = &job
	return d
}

func encryptSegment(t *testing.T, key []byte, seq int64, plain []byte) []byte {
	t.Helper()
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	data := append(append([]byte(nil), plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	iv := make([]byte, 16)
	binary.BigEndian.PutUint64(iv[8:], uint64(seq))
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(data, data)
	return data
}

func readResult(t *testing.T, d *Download) []byte {
	t.Helper()
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("status %s: %s", info.Status, info.Error)
	}
	data, err := os.ReadFile(filepath.Join(info.Dir, info.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(info.Dir, info.FileName) + ".odmparts"); !os.IsNotExist(err) {
		t.Fatal("work folder left behind")
	}
	if info.Done != int64(len(data)) || info.Total != int64(len(data)) {
		t.Fatalf("progress %d/%d for a %d-byte file", info.Done, info.Total, len(data))
	}
	return data
}

func TestStreamHLSEncrypted(t *testing.T) {
	key := []byte("0123456789abcdef")
	var want []byte
	segs := map[string][]byte{}
	for i := 0; i < 4; i++ {
		plain := []byte(strings.Repeat(fmt.Sprintf("segment-%d|", i), 50+i))
		want = append(want, plain...)
		segs["/s"+strconv.Itoa(i)+".ts"] = encryptSegment(t, key, int64(5+i), plain)
	}
	var lowFetched atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=500000\nhigh.m3u8\n")
	})
	mux.HandleFunc("/low.m3u8", func(w http.ResponseWriter, r *http.Request) { lowFetched.Store(true) })
	mux.HandleFunc("/high.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:5\n#EXT-X-KEY:METHOD=AES-128,URI=\"/key\"\n")
		for i := 0; i < 4; i++ {
			fmt.Fprintf(w, "#EXTINF:4,\ns%d.ts\n", i)
		}
		fmt.Fprint(w, "#EXT-X-ENDLIST\n")
	})
	mux.HandleFunc("/key", func(w http.ResponseWriter, r *http.Request) { w.Write(key) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if b, ok := segs[r.URL.Path]; ok {
			w.Write(b)
			return
		}
		http.NotFound(w, r)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	d := newStreamDownload(t, srv.Client(), 3, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/master.m3u8"})
	d.Start(context.Background())
	got := readResult(t, d)
	if !bytes.Equal(got, want) {
		t.Fatalf("decrypted output differs (%d vs %d bytes)", len(got), len(want))
	}
	if d.Info().FileName != "clip.ts" {
		t.Fatalf("name %q", d.Info().FileName)
	}
	if lowFetched.Load() {
		t.Fatal("fetched the low variant instead of the best one")
	}
}

// TestStreamResumeAndExpiry pauses half way, then resumes: fetched
// segments are not requested again, and a segment whose signed URL
// expired is retried after the playlist is reloaded.
func TestStreamResumeAndExpiry(t *testing.T) {
	var gen atomic.Int64
	var mu sync.Mutex
	hits := map[string]int{}
	reached := make(chan struct{}, 1)
	var block atomic.Bool
	block.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/p.m3u8", func(w http.ResponseWriter, r *http.Request) {
		g := gen.Add(1)
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n")
		for i := 0; i < 4; i++ {
			fmt.Fprintf(w, "#EXTINF:2,\nseg%d.ts?tok=%d\n", i, g)
		}
		fmt.Fprint(w, "#EXT-X-ENDLIST\n")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		tok, _ := strconv.ParseInt(r.URL.Query().Get("tok"), 10, 64)
		mu.Lock()
		hits[name]++
		mu.Unlock()
		switch {
		case name == "seg2.ts" && block.Load():
			reached <- struct{}{}
			<-r.Context().Done() // hang until the client gives up (pause)
			return
		case name == "seg3.ts" && tok < 3:
			http.Error(w, "expired", http.StatusForbidden)
			return
		}
		fmt.Fprintf(w, "[%s]", name)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"})
	done := make(chan struct{})
	go func() { d.Start(context.Background()); close(done) }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("segment 2 never requested")
	}
	d.Pause()
	<-done
	if s := d.Info().Status; s != StatusPaused {
		t.Fatalf("after pause: %s", s)
	}
	// Persist and restore, as a restart of ODM would.
	d = Restore(d.State(), srv.Client())
	block.Store(false)
	d.Start(context.Background())
	got := readResult(t, d)
	if string(got) != "[seg0.ts][seg1.ts][seg2.ts][seg3.ts]" {
		t.Fatalf("output %q", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits["seg0.ts"] != 1 || hits["seg1.ts"] != 1 {
		t.Fatalf("finished segments fetched again: %v", hits)
	}
	if hits["seg3.ts"] != 2 { // refused once, then fetched with the new token
		t.Fatalf("expiry retry: %v", hits)
	}
}

func TestStreamFMP4ByteRanges(t *testing.T) {
	content := []byte("INITAAAAAABBBBBBCCCCC")
	mux := http.NewServeMux()
	mux.HandleFunc("/v.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MAP:URI=\"all.mp4\",BYTERANGE=\"4@0\"\n"+
			"#EXTINF:2,\n#EXT-X-BYTERANGE:6@4\nall.mp4\n#EXTINF:2,\n#EXT-X-BYTERANGE:6\nall.mp4\n"+
			"#EXTINF:2,\n#EXT-X-BYTERANGE:5\nall.mp4\n#EXT-X-ENDLIST\n")
	})
	mux.HandleFunc("/all.mp4", func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "all.mp4", time.Time{}, bytes.NewReader(content))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	d := newStreamDownload(t, srv.Client(), 4, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/v.m3u8"})
	d.Start(context.Background())
	if got := readResult(t, d); !bytes.Equal(got, content) {
		t.Fatalf("output %q", got)
	}
	if d.Info().FileName != "clip.mp4" {
		t.Fatalf("name %q", d.Info().FileName)
	}
}

const twoTrackMPD = `<?xml version="1.0"?>
<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" type="static" mediaPresentationDuration="PT4S">
  <Period>
    <AdaptationSet mimeType="video/mp4">
      <SegmentTemplate media="v-$Number$.m4s" initialization="v-init.mp4" timescale="1" duration="2"/>
      <Representation id="v1" bandwidth="1000"/>
      <Representation id="v2" bandwidth="9000"/>
    </AdaptationSet>
    <AdaptationSet mimeType="audio/mp4">
      <SegmentTemplate media="a-$Number$.m4s" initialization="a-init.mp4" timescale="1" duration="2"/>
      <Representation id="a1" bandwidth="128"/>
    </AdaptationSet>
  </Period>
</MPD>`

func TestStreamDASHNeedsFFmpeg(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/m.mpd", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, twoTrackMPD) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, r.URL.Path) })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	setFFmpegPath(filepath.Join(t.TempDir(), "no-ffmpeg-here"))
	defer setFFmpegPath("")
	d := newStreamDownload(t, srv.Client(), 4, StreamJob{Kind: media.KindDASH, Manifest: srv.URL + "/m.mpd"})
	d.Start(context.Background())
	info := d.Info()
	if info.Status != StatusError || !strings.Contains(info.Error, "ffmpeg") {
		t.Fatalf("status %s: %s", info.Status, info.Error)
	}
	// The segments stay for a retry once ffmpeg is available.
	work := filepath.Join(info.Dir, info.FileName+".odmparts")
	for _, p := range []string{"t0/init0.seg", "t0/1.seg", "t1/init0.seg", "t1/1.seg"} {
		if _, err := os.Stat(filepath.Join(work, p)); err != nil {
			t.Fatalf("missing %s: %v", p, err)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(work, "t0", "0.seg")); string(b) != "/v-1.m4s" {
		t.Fatalf("best video not chosen: %q", b)
	}
}

func TestStreamRefusals(t *testing.T) {
	playlists := map[string]string{
		"/live.m3u8": "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\na.ts\n",
		"/drm.m3u8":  "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"skd://k\",KEYFORMAT=\"com.apple.streamingkeydelivery\"\n#EXTINF:2,\na.ts\n#EXT-X-ENDLIST\n",
	}
	var segCount atomic.Int64
	segCount.Store(3)
	mux := http.NewServeMux()
	mux.HandleFunc("/grow.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n")
		for i := int64(0); i < segCount.Load(); i++ {
			fmt.Fprintf(w, "#EXTINF:2,\ns%d.ts\n", i)
		}
		fmt.Fprint(w, "#EXT-X-ENDLIST\n")
	})
	mux.HandleFunc("/s2.ts", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "x", http.StatusInternalServerError) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if p, ok := playlists[r.URL.Path]; ok {
			fmt.Fprint(w, p)
			return
		}
		fmt.Fprint(w, "data")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	for path, want := range map[string]string{"/live.m3u8": "live", "/drm.m3u8": "DRM"} {
		d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + path})
		d.Start(context.Background())
		if info := d.Info(); info.Status != StatusError || !strings.Contains(info.Error, want) {
			t.Errorf("%s: %s %q", path, info.Status, info.Error)
		}
	}

	// A playlist that changes between runs cannot be resumed safely.
	d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/grow.m3u8"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d.Start(ctx) // fails on s2 after its retries, or times out: either way unfinished
	segCount.Store(4)
	d.Start(context.Background())
	if info := d.Info(); info.Status != StatusError || !strings.Contains(info.Error, "changed") {
		t.Fatalf("changed playlist: %s %q", info.Status, info.Error)
	}
}

// TestStreamDASHMergeRealFFmpeg joins a generated DASH stream with a real
// ffmpeg; it runs when ffmpeg is on PATH or ODM_TEST_FFMPEG names it.
func TestStreamDASHMergeRealFFmpeg(t *testing.T) {
	bin := os.Getenv("ODM_TEST_FFMPEG")
	if bin == "" {
		var err error
		if bin, err = exec.LookPath("ffmpeg"); err != nil {
			t.Skip("no ffmpeg")
		}
	}
	src := t.TempDir()
	cmd := exec.Command(bin, "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=duration=4:size=160x120:rate=10",
		"-f", "lavfi", "-i", "sine=duration=4",
		"-map", "0:v", "-map", "1:a", "-c:v", "libx264", "-g", "10", "-c:a", "aac",
		"-f", "dash", "-seg_duration", "1", "-use_template", "1", "-use_timeline", "0",
		filepath.Join(src, "m.mpd"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("cannot generate DASH input: %v %s", err, out)
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(src)))
	defer srv.Close()

	setFFmpegPath(bin)
	defer setFFmpegPath("")
	d := newStreamDownload(t, srv.Client(), 4, StreamJob{Kind: media.KindDASH, Manifest: srv.URL + "/m.mpd"})
	d.Start(context.Background())
	info := d.Info()
	if info.Status != StatusComplete {
		t.Fatalf("%s: %s", info.Status, info.Error)
	}
	out := filepath.Join(info.Dir, info.FileName)
	probe, _ := exec.Command(bin, "-hide_banner", "-i", out).CombinedOutput()
	s := string(probe)
	if !strings.Contains(s, "Video: h264") || !strings.Contains(s, "Audio: aac") || !strings.Contains(s, "Duration: 00:00:04") {
		t.Fatalf("merged file %s:\n%s", info.FileName, s)
	}
}

// Regressions found in review.

func segServer(t *testing.T, playlist string, extra map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/p.m3u8", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, playlist) })
	for p, h := range extra {
		mux.HandleFunc(p, h)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "[%s]", r.URL.Path) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

const threeSegs = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nseg0.ts\n#EXTINF:2,\nseg1.ts\n#EXTINF:2,\nseg2.ts\n#EXT-X-ENDLIST\n"

func TestStreamIgnoresStaleWorkFolder(t *testing.T) {
	srv := segServer(t, threeSegs, nil)
	d := newStreamDownload(t, srv.Client(), 2, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"})
	stale := filepath.Join(d.BaseDir, "clip.ts.odmparts", "t0")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(stale, "0.seg"), []byte("[STALE]"), 0o644)
	d.Start(context.Background())
	if got := string(readResult(t, d)); got != "[/seg0.ts][/seg1.ts][/seg2.ts]" {
		t.Fatalf("output %q", got)
	}
}

func TestStreamRemoveDeletesWorkFolder(t *testing.T) {
	srv := segServer(t, threeSegs, map[string]http.HandlerFunc{
		"/seg2.ts": func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusNotFound) },
	})
	m := NewManager(DefaultSettings(t.TempDir()), "")
	info := m.Create(AddOptions{URL: srv.URL + "/p.m3u8", FileName: "clip", Stream: &StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"}})
	d := m.get(info.ID)
	d.client = srv.Client()
	d.Start(context.Background()) // fails on seg2, keeps seg0/seg1
	work := filepath.Join(d.Dir, d.FileName+".odmparts")
	if _, err := os.Stat(work); err != nil {
		t.Fatalf("no work folder to remove: %v", err)
	}
	m.Remove(info.ID)
	if _, err := os.Stat(work); !os.IsNotExist(err) {
		t.Fatal("work folder left behind after Remove")
	}
}

func TestStreamFinishUnderFreeName(t *testing.T) {
	var d *Download
	srv := segServer(t, threeSegs, map[string]http.HandlerFunc{
		"/seg2.ts": func(w http.ResponseWriter, r *http.Request) {
			// Someone creates the target name while we download.
			os.WriteFile(filepath.Join(d.Dir, "clip.ts"), []byte("other"), 0o644)
			fmt.Fprint(w, "[/seg2.ts]")
		},
	})
	d = newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"})
	d.Start(context.Background())
	if got := string(readResult(t, d)); got != "[/seg0.ts][/seg1.ts][/seg2.ts]" || d.Info().FileName != "clip (2).ts" {
		t.Fatalf("%q as %s", got, d.Info().FileName)
	}
	if _, err := os.Stat(filepath.Join(d.Dir, "clip.ts.odmparts")); !os.IsNotExist(err) {
		t.Fatal("work folder of the original name left behind")
	}
}

func TestStreamCookiesStayOnSite(t *testing.T) {
	var leaked, kept atomic.Value
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked.Store(r.Header.Get("Cookie") + "|" + r.Header.Get("Authorization"))
		fmt.Fprint(w, "[x]")
	}))
	defer other.Close()
	// Same host as the manifest, different port: same site.
	srv := segServer(t, "", nil)
	playlist := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nseg0.ts\n#EXTINF:2,\n" +
		strings.Replace(other.URL, "127.0.0.1", "localhost", 1) + "/x.ts\n#EXT-X-ENDLIST\n"
	mux := http.NewServeMux()
	mux.HandleFunc("/p.m3u8", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, playlist) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		kept.Store(r.Header.Get("Cookie"))
		fmt.Fprint(w, "[s]")
	})
	srv.Config.Handler = mux
	d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"})
	d.Headers = map[string]string{"Cookie": "session=SECRET", "authorization": "Bearer T", "Referer": "https://site/"}
	d.Start(context.Background())
	readResult(t, d)
	if v, _ := leaked.Load().(string); v != "|" {
		t.Fatalf("credentials sent to another host: %q", v)
	}
	if v, _ := kept.Load().(string); v != "session=SECRET" {
		t.Fatalf("cookie not sent to the manifest's own host: %q", v)
	}
}

func TestSameSite(t *testing.T) {
	u := func(s string) *url.URL { x, _ := url.Parse(s); return x }
	for _, c := range []struct {
		m, u string
		want bool
	}{
		{"https://www.example.com/a.m3u8", "https://cdn.example.com/s.ts", true},
		{"https://a.example.co.uk/a.m3u8", "https://b.example.co.uk/s.ts", true},
		{"https://a.example.co.uk/a.m3u8", "https://evil.co.uk/s.ts", false},
		{"https://example.com/a.m3u8", "https://example.com.evil.net/s.ts", false},
		{"http://127.0.0.1:1/a", "http://127.0.0.1:2/b", true},
		{"http://127.0.0.1/a", "http://localhost/b", false},
	} {
		if got := sameSite(c.m, u(c.u)); got != c.want {
			t.Errorf("sameSite(%s, %s) = %v", c.m, c.u, got)
		}
	}
}

func TestStreamReaddress(t *testing.T) {
	srv := segServer(t, threeSegs, nil)
	m := NewManager(DefaultSettings(t.TempDir()), "")
	info := m.Create(AddOptions{URL: srv.URL + "/old.m3u8", FileName: "clip", Stream: &StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/old.m3u8"}})
	d := m.get(info.ID)
	d.client = srv.Client()
	d.Start(context.Background()) // old address is a 404 playlist… served as "[/old.m3u8]"
	if d.Info().Status != StatusError {
		t.Fatalf("old address should fail: %s", d.Info().Status)
	}
	if err := m.Readdress(context.Background(), info.ID, srv.URL+"/p.m3u8"); err != nil {
		t.Fatal(err)
	}
	if d.Info().Status != StatusPaused {
		t.Fatalf("status after readdress: %s", d.Info().Status)
	}
	d.Start(context.Background())
	if got := string(readResult(t, d)); got != "[/seg0.ts][/seg1.ts][/seg2.ts]" {
		t.Fatalf("output %q", got)
	}

	fixed := m.Create(AddOptions{URL: srv.URL + "/p.m3u8", Stream: &StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/m.m3u8", Video: srv.URL + "/p.m3u8"}})
	m.Pause(fixed.ID)
	if err := m.Readdress(context.Background(), fixed.ID, srv.URL+"/x.m3u8"); err == nil {
		t.Fatal("readdress of a fixed playlist accepted")
	}
}

func TestStreamRestoreShowsProgress(t *testing.T) {
	srv := segServer(t, threeSegs, nil)
	d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"})
	d.Start(context.Background())
	before := d.Info()
	after := Restore(d.State(), srv.Client()).Info()
	if after.Done != before.Done || after.Progress != 100 {
		t.Fatalf("after restore %d/%d (%.0f%%), before %d", after.Done, after.Total, after.Progress, before.Done)
	}
}

func TestStreamPinnedVariant(t *testing.T) {
	var hiBW atomic.Int64
	hiBW.Store(500000)
	mux := http.NewServeMux()
	mux.HandleFunc("/master.m3u8", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100000\nlow.m3u8?sig=%d\n#EXT-X-STREAM-INF:BANDWIDTH=%d\nhigh.m3u8?sig=%d\n",
			time.Now().UnixNano(), hiBW.Load(), time.Now().UnixNano())
	})
	mux.HandleFunc("/high.m3u8", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, threeSegs) })
	mux.HandleFunc("/low.m3u8", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, threeSegs) })
	mux.HandleFunc("/seg2.ts", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "x", http.StatusNotFound) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "x") })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/master.m3u8"})
	d.Start(context.Background()) // picks high, fails on seg2
	if d.Stream.Variant == "" {
		t.Fatal("chosen variant not recorded")
	}
	// The ladder changes: high is now a different rendition. Resuming
	// must not silently switch to it or to low.
	hiBW.Store(900000)
	d.Start(context.Background())
	if info := d.Info(); info.Status != StatusError || !strings.Contains(info.Error, "no longer offered") {
		t.Fatalf("%s: %s", info.Status, info.Error)
	}
}

func TestStreamEncryptedInitAndKeyExpiry(t *testing.T) {
	key := []byte("0123456789abcdef")
	iv := bytes.Repeat([]byte{7}, 16)
	block, _ := aes.NewCipher(key)
	enc := func(plain []byte, iv []byte) []byte {
		pad := aes.BlockSize - len(plain)%aes.BlockSize
		data := append(append([]byte(nil), plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
		cipher.NewCBCEncrypter(block, iv).CryptBlocks(data, data)
		return data
	}
	var keyHits atomic.Int64
	var gen atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/p.m3u8", func(w http.ResponseWriter, r *http.Request) {
		g := gen.Add(1)
		fmt.Fprintf(w, "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-KEY:METHOD=AES-128,URI=\"key?g=%d\",IV=0x%x\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:2,\ns.m4s\n#EXT-X-ENDLIST\n", g, iv)
	})
	mux.HandleFunc("/key", func(w http.ResponseWriter, r *http.Request) {
		keyHits.Add(1)
		if r.URL.Query().Get("g") == "1" { // the first key URL has expired
			http.Error(w, "expired", http.StatusForbidden)
			return
		}
		w.Write(key)
	})
	mux.HandleFunc("/init.mp4", func(w http.ResponseWriter, r *http.Request) { w.Write(enc([]byte("INIT"), iv)) })
	mux.HandleFunc("/s.m4s", func(w http.ResponseWriter, r *http.Request) { w.Write(enc([]byte("MEDIA"), iv)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"})
	d.Start(context.Background())
	if got := string(readResult(t, d)); got != "INITMEDIA" {
		t.Fatalf("output %q", got)
	}
}

func TestStreamNoPresizeAndEmptySegmentRefetched(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	srv := segServer(t, threeSegs, map[string]http.HandlerFunc{
		"/seg2.ts": func(w http.ResponseWriter, r *http.Request) {
			if fail.Load() {
				http.Error(w, "x", http.StatusNotFound)
				return
			}
			fmt.Fprint(w, "[/seg2.ts]")
		},
	})
	d := newStreamDownload(t, srv.Client(), 1, StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8", Estimate: 1 << 30})
	d.Start(context.Background())
	// Restored with a 1 GiB estimate: the .odmpart must not be pre-sized.
	d = Restore(d.State(), srv.Client())
	// A crash left an empty segment file behind.
	seg1 := filepath.Join(d.Dir, d.FileName+".odmparts", "t0", "1.seg")
	os.WriteFile(seg1, nil, 0o644)
	fail.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d.Start(ctx) // opens the target, then stops: check the size
	if n := fileSize(filepath.Join(d.Dir, d.FileName+".odmpart")); n != 0 {
		t.Fatalf(".odmpart pre-sized to %d", n)
	}
	d.Start(context.Background())
	if got := string(readResult(t, d)); got != "[/seg0.ts][/seg1.ts][/seg2.ts]" {
		t.Fatalf("output %q", got)
	}
}

func TestSameSiteIPv4(t *testing.T) {
	u, _ := url.Parse("http://192.168.0.1/s.ts")
	if sameSite("http://10.0.0.1/m.m3u8", u) {
		t.Fatal("different IPv4 addresses treated as one site")
	}
}

func TestPickHLSSessionPath(t *testing.T) {
	first := &media.HLSMaster{Variants: []media.HLSVariant{
		{URL: "https://h/chunklist_w1_b500.m3u8", Bandwidth: 500, Width: 1280, Height: 720},
		{URL: "https://h/chunklist_w1_b100.m3u8", Bandwidth: 100, Width: 640, Height: 360},
	}}
	v, _, err := pickHLS(first, "", "")
	if err != nil {
		t.Fatal(err)
	}
	key := hlsVariantKey(*v)
	second := &media.HLSMaster{Variants: []media.HLSVariant{
		{URL: "https://h/chunklist_w2_b100.m3u8", Bandwidth: 100, Width: 640, Height: 360},
		{URL: "https://h/chunklist_w2_b500.m3u8", Bandwidth: 500, Width: 1280, Height: 720},
	}}
	got, _, err := pickHLS(second, key, "")
	if err != nil || got.URL != "https://h/chunklist_w2_b500.m3u8" {
		t.Fatalf("session path: %v %v", got, err)
	}
	// Two candidates that differ only by path: ambiguous, refused.
	twins := &media.HLSMaster{Variants: []media.HLSVariant{
		{URL: "https://h/a.m3u8", Bandwidth: 500, Width: 1280, Height: 720},
		{URL: "https://h/b.m3u8", Bandwidth: 500, Width: 1280, Height: 720},
	}}
	if _, _, err := pickHLS(twins, key, ""); err == nil {
		t.Fatal("ambiguous match accepted")
	}
}

func TestRemoveWaitsForRun(t *testing.T) {
	release := make(chan struct{})
	srv := segServer(t, threeSegs, map[string]http.HandlerFunc{
		"/seg1.ts": func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		},
	})
	m := NewManager(DefaultSettings(t.TempDir()), "")
	info := m.Create(AddOptions{URL: srv.URL + "/p.m3u8", FileName: "clip", Stream: &StreamJob{Kind: media.KindHLS, Manifest: srv.URL + "/p.m3u8"}})
	d := m.get(info.ID)
	d.client = srv.Client()
	go d.Start(context.Background())
	work := ""
	for i := 0; i < 200 && work == ""; i++ {
		time.Sleep(10 * time.Millisecond)
		if p := filepath.Join(d.Info().Dir, d.Info().FileName+".odmparts"); fileSize(filepath.Join(p, "t0", "0.seg")) > 0 {
			work = p
		}
	}
	if work == "" {
		t.Fatal("download never got going")
	}
	m.Remove(info.ID)
	close(release)
	if _, err := os.Stat(work); !os.IsNotExist(err) {
		t.Fatal("work folder left behind")
	}
	select {
	case <-d.runDone:
	default:
		t.Fatal("Remove returned before the run ended")
	}
}
