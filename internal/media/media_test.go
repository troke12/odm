package media

import (
	"bytes"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func load(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestDetect(t *testing.T) {
	cases := []struct {
		ct, url, body string
		want          Kind
	}{
		// Body wins over a wrong label and a bare URL.
		{"text/plain", "https://x/play", "\xef\xbb\xbf  #EXTM3U\n#EXTINF:1,\na.ts", KindHLS},
		{"application/octet-stream", "https://x/get?id=1", `<?xml version="1.0"?><!-- c --><MPD xmlns="urn:mpeg:dash:schema:mpd:2011">`, KindDASH},
		{"", "", `<?xml version="1.0"?><SmoothStreamingMedia MajorVersion="2">`, KindSmooth},
		{"", "", `<manifest xmlns="http://ns.adobe.com/f4m/1.0">`, KindF4M},
		{"", "", `<manifest xmlns="urn:other">`, KindNone},
		{"", "", `<html><body>`, KindNone},
		// Content-Type, with parameters and case.
		{"Application/vnd.apple.mpegURL; charset=utf-8", "", "", KindHLS},
		{"audio/mpegurl", "", "", KindHLS}, // a playlist, not audio
		{"application/dash+xml", "", "", KindDASH},
		{"video/mp4", "", "", KindFile},
		{"audio/webm", "", "", KindFile},
		{"video/mp2t", "", "", KindFile},
		// URL as the last resort.
		{"", "https://cdn/x/master.M3U8?sig=1", "", KindHLS},
		{"", "https://cdn/x/stream.mpd", "", KindDASH},
		{"", "https://cdn/x/clip.ism/Manifest", "", KindSmooth},
		{"", "https://cdn/x/clip.webm", "", KindFile},
		{"application/octet-stream", "https://cdn/x/page.html", "", KindNone},
		{"", "::bad url", "", KindNone},
	}
	for _, c := range cases {
		if got := Detect(c.ct, c.url, []byte(c.body)); got != c.want {
			t.Errorf("Detect(%q, %q, %.20q) = %q, want %q", c.ct, c.url, c.body, got, c.want)
		}
	}
	if !KindDASH.IsManifest() || KindFile.IsManifest() || KindNone.IsManifest() {
		t.Error("IsManifest")
	}
	// Only the first SniffLen bytes are looked at.
	late := append(bytes.Repeat([]byte(" "), SniffLen), "#EXTM3U"...)
	if Detect("", "", late) != KindNone {
		t.Error("sniffed beyond SniffLen")
	}
}

func TestByteRangeHeader(t *testing.T) {
	if h := (ByteRange{Offset: 720, Length: 1000}).Header(); h != "bytes=720-1719" {
		t.Fatal(h)
	}
}

func TestHLSMaster(t *testing.T) {
	p, err := ParseHLS(mustURL("https://example.com/vod/master.m3u8"), load(t, "hls_master.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	if p.Media != nil || p.Master == nil {
		t.Fatal("not a master playlist")
	}
	m := p.Master
	if len(m.Variants) != 3 { // the I-frame-only stream is not a variant
		t.Fatalf("%d variants", len(m.Variants))
	}
	v := m.Variants[0]
	if v.URL != "https://example.com/vod/video/1080p/index.m3u8" || v.Bandwidth != 5000000 ||
		v.AverageBandwidth != 4200000 || v.Width != 1920 || v.Height != 1080 ||
		v.FrameRate != 29.97 || v.Codecs != "avc1.640028,mp4a.40.2" || v.Audio != "aud" {
		t.Fatalf("variant 0: %+v", v)
	}
	if u := m.Variants[2].URL; u != "https://other.example.net/360p.m3u8?token=a,b" {
		t.Fatalf("absolute URI: %s", u)
	}
	if len(m.Renditions) != 3 {
		t.Fatalf("%d renditions", len(m.Renditions))
	}
	en, de, cc := m.Renditions[0], m.Renditions[1], m.Renditions[2]
	if en.Type != "AUDIO" || en.GroupID != "aud" || !en.Default || !en.Autoselect ||
		en.URL != "https://example.com/vod/audio/en/index.m3u8" {
		t.Fatalf("en: %+v", en)
	}
	if de.Name != "Deutsch, Stereo" || de.Default || de.Language != "de" {
		t.Fatalf("quoted comma: %+v", de)
	}
	if cc.URL != "" {
		t.Fatalf("muxed captions got a URL: %+v", cc)
	}
}

func TestHLSMedia(t *testing.T) {
	p, err := ParseHLS(mustURL("https://example.com/vod/v/index.m3u8"), load(t, "hls_media.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	m := p.Media
	if m == nil || p.Master != nil {
		t.Fatal("not a media playlist")
	}
	if m.TargetDuration != 6*time.Second || m.MediaSequence != 7 || m.PlaylistType != "VOD" ||
		!m.Ended || m.Live() || m.Protected {
		t.Fatalf("header: %+v", m)
	}
	if len(m.Segments) != 4 {
		t.Fatalf("%d segments", len(m.Segments))
	}
	if d := m.Duration(); d != 17506*time.Millisecond {
		t.Fatalf("duration %v", d)
	}
	base := "https://example.com/vod/v/"
	s0, s1, s2, s3 := m.Segments[0], m.Segments[1], m.Segments[2], m.Segments[3]

	if s0.URL != base+"seg7.m4s" || s0.Sequence != 7 || s0.Duration != 6006*time.Millisecond || s0.Range != nil {
		t.Fatalf("seg 0: %+v", s0)
	}
	if s0.Map == nil || s0.Map.URL != base+"init.mp4" || *s0.Map.Range != (ByteRange{0, 720}) {
		t.Fatalf("map: %+v", s0.Map)
	}
	if s0.Key == nil || s0.Key.URL != "https://keys.example.com/k1" || s0.Key.IV != nil {
		t.Fatalf("key 0: %+v", s0.Key)
	}
	wantIV := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7}
	if !bytes.Equal(s0.Key.IVFor(s0.Sequence), wantIV) {
		t.Fatalf("derived IV %x", s0.Key.IVFor(s0.Sequence))
	}

	if s1.URL != base+"all.m4s" || *s1.Range != (ByteRange{720, 1000}) || s1.Duration != 5500*time.Millisecond {
		t.Fatalf("seg 1: %+v", s1)
	}
	if s1.Key.URL != base+"k2.bin" || s1.Key.IVFor(8)[15] != 0x0f || s1.Key.IVFor(8)[0] != 0 {
		t.Fatalf("explicit IV: %+v", s1.Key)
	}
	// No offset: continues after the previous range of the same file.
	if *s2.Range != (ByteRange{1720, 500}) || s2.Key != s1.Key || s2.Sequence != 9 {
		t.Fatalf("seg 2: %+v", s2)
	}
	if s3.Key != nil || !s3.Discontinuity || s3.Map != s0.Map || s3.Sequence != 10 {
		t.Fatalf("seg 3: %+v", s3)
	}
}

func TestHLSLiveAndProtected(t *testing.T) {
	live := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:100\n#EXTINF:2,\nlive100.ts\n"
	p, err := ParseHLS(nil, []byte(live))
	if err != nil {
		t.Fatal(err)
	}
	if !p.Media.Live() || p.Media.Segments[0].URL != "live100.ts" || p.Media.Segments[0].Sequence != 100 {
		t.Fatalf("live: %+v", p.Media)
	}

	p, err = ParseHLS(nil, load(t, "hls_drm.m3u8"))
	if err != nil {
		t.Fatal(err)
	}
	if !p.Media.Protected || p.Media.Segments[0].Key != nil {
		t.Fatalf("DRM playlist not flagged: %+v", p.Media)
	}

	// An identity key next to a DRM key: still downloadable.
	both := "#EXTM3U\n#EXT-X-TARGETDURATION:2\n" +
		`#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://x",KEYFORMAT="com.apple.streamingkeydelivery"` + "\n" +
		`#EXT-X-KEY:METHOD=AES-128,URI="k"` + "\n#EXTINF:2,\na.ts\n"
	p, err = ParseHLS(nil, []byte(both))
	if err != nil {
		t.Fatal(err)
	}
	if p.Media.Protected || p.Media.Segments[0].Key == nil {
		t.Fatalf("identity key ignored: %+v", p.Media)
	}
}

func TestHLSErrors(t *testing.T) {
	for name, in := range map[string]string{
		"not hls":     "<html>",
		"empty":       "",
		"no content":  "#EXTM3U\n#EXT-X-VERSION:3\n",
		"bad extinf":  "#EXTM3U\n#EXTINF:abc,\na.ts\n",
		"bad range":   "#EXTM3U\n#EXT-X-BYTERANGE:-5\n#EXTINF:1,\na.ts\n",
		"bad iv":      "#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"k\",IV=0x12\n#EXTINF:1,\na.ts\n",
		"master only": "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\n",
	} {
		if _, err := ParseHLS(nil, []byte(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDASHTemplate(t *testing.T) {
	m, err := ParseDASH(mustURL("https://example.com/vod/manifest.mpd"), load(t, "dash_template.mpd"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Live || m.Duration != 10500*time.Millisecond || len(m.Periods) != 1 {
		t.Fatalf("manifest: %+v", m)
	}
	reps := m.Periods[0].Representations
	if len(reps) != 3 {
		t.Fatalf("%d representations", len(reps))
	}
	v720, v1080, a := reps[0], reps[1], reps[2]
	base := "https://example.com/vod/media/"
	// 10.5 s in 4 s segments: 3 segments, the last one partial.
	if len(v720.Segments) != 3 || v720.Segments[0].URL != base+"v720/seg-00001.m4s" ||
		v720.Segments[2].URL != base+"v720/seg-00003.m4s" || v720.Segments[0].Duration != 4*time.Second {
		t.Fatalf("v720 segments: %+v", v720.Segments)
	}
	if v720.Init == nil || v720.Init.URL != base+"v720/init.mp4" {
		t.Fatalf("v720 init: %+v", v720.Init)
	}
	if !v720.IsVideo() || v720.IsAudio() || v720.Codecs != "avc1.4d401f" || v720.FrameRate != "30000/1001" ||
		v720.Width != 1280 || v720.Height != 720 || v720.Bandwidth != 2400000 {
		t.Fatalf("v720 inherited attributes: %+v", v720)
	}
	if v1080.Codecs != "avc1.640028" { // own attribute overrides the set's
		t.Fatalf("v1080 codecs %q", v1080.Codecs)
	}
	if !a.IsAudio() || a.Lang != "en" || len(a.Segments) != 3 || a.Segments[0].URL != base+"a/128000/1.m4s$" {
		t.Fatalf("audio: %+v", a)
	}
}

func TestDASHTimeline(t *testing.T) {
	m, err := ParseDASH(mustURL("https://example.com/m.mpd"), load(t, "dash_timeline.mpd"))
	if err != nil {
		t.Fatal(err)
	}
	r := m.Periods[0].Representations[0]
	// 3×4 s + 2 s, then 1 s repeated to the 20 s period end: 6 more.
	if len(r.Segments) != 10 {
		t.Fatalf("%d segments", len(r.Segments))
	}
	var total time.Duration
	for _, s := range r.Segments {
		total += s.Duration
	}
	if total != 20*time.Second {
		t.Fatalf("total %v", total)
	}
	want := map[int]string{0: "t-0.webm", 3: "t-1080000.webm", 4: "t-1260000.webm", 9: "t-1710000.webm"}
	for i, name := range want {
		if r.Segments[i].URL != "https://cdn.example.org/v/"+name {
			t.Errorf("segment %d: %s", i, r.Segments[i].URL)
		}
	}
	if r.Init.URL != "https://cdn.example.org/v/init.webm" || !r.IsVideo() {
		t.Fatalf("init/kind: %+v", r)
	}
}

func TestDASHListAndBase(t *testing.T) {
	m, err := ParseDASH(mustURL("https://example.com/a/m.mpd"), load(t, "dash_list_base.mpd"))
	if err != nil {
		t.Fatal(err)
	}
	list, whole := m.Periods[0].Representations[0], m.Periods[0].Representations[1]
	if list.Init == nil || list.Init.URL != "https://example.com/a/list/file.mp4" || *list.Init.Range != (ByteRange{0, 800}) {
		t.Fatalf("list init: %+v", list.Init)
	}
	if len(list.Segments) != 2 || *list.Segments[0].Range != (ByteRange{800, 50000}) ||
		list.Segments[1].URL != "https://example.com/a/list/part2.mp4" || list.Segments[1].Range != nil ||
		list.Segments[0].Duration != 30*time.Second {
		t.Fatalf("list segments: %+v", list.Segments)
	}
	if list.Protected {
		t.Fatal("unprotected track flagged")
	}
	if !whole.Protected || len(whole.Segments) != 1 || whole.Init != nil ||
		whole.Segments[0].URL != "https://example.com/a/audio/whole.m4a" || whole.Segments[0].Duration != time.Minute {
		t.Fatalf("SegmentBase: %+v", whole)
	}
}

func TestDASHLiveAndLimits(t *testing.T) {
	live := `<MPD type="dynamic"><Period><AdaptationSet mimeType="video/mp4">
		<SegmentTemplate media="$Number$.m4s" duration="2"/><Representation id="v"/></AdaptationSet></Period></MPD>`
	m, err := ParseDASH(mustURL("https://example.com/live.mpd"), []byte(live))
	if err != nil {
		t.Fatal(err)
	}
	if !m.Live || len(m.Periods[0].Representations[0].Segments) != 0 {
		t.Fatalf("live: %+v", m)
	}

	huge := `<MPD mediaPresentationDuration="PT1000H"><Period><AdaptationSet mimeType="video/mp4">
		<SegmentTemplate media="$Number$.m4s" duration="1" timescale="1000"/><Representation id="v"/></AdaptationSet></Period></MPD>`
	if _, err := ParseDASH(nil, []byte(huge)); err == nil || !strings.Contains(err.Error(), "segments") {
		t.Fatalf("unbounded expansion: %v", err)
	}

	for name, in := range map[string]string{
		"not xml":       "#EXTM3U",
		"no period":     `<MPD/>`,
		"months":        `<MPD mediaPresentationDuration="P1M"><Period/></MPD>`,
		"bad ident":     `<MPD mediaPresentationDuration="PT4S"><Period><AdaptationSet><SegmentTemplate media="$Foo$" duration="2"/><Representation id="v"/></AdaptationSet></Period></MPD>`,
		"no length":     `<MPD><Period><AdaptationSet><SegmentTemplate media="$Number$" duration="2"/><Representation id="v"/></AdaptationSet></Period></MPD>`,
		"no base":       `<MPD><Period><AdaptationSet><Representation id="v"/></AdaptationSet></Period></MPD>`,
		"bad range":     `<MPD><Period><AdaptationSet><Representation id="v"><SegmentList><SegmentURL media="a" mediaRange="9-1"/></SegmentList></Representation></AdaptationSet></Period></MPD>`,
		"zero duration": `<MPD><Period><AdaptationSet><SegmentTemplate media="$Time$"><SegmentTimeline><S d="0"/></SegmentTimeline></SegmentTemplate><Representation id="v"/></AdaptationSet></Period></MPD>`,
	} {
		if _, err := ParseDASH(nil, []byte(in)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestISODuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"PT0H0M10.5S": 10500 * time.Millisecond,
		"PT1H":        time.Hour,
		"P1DT2M":      24*time.Hour + 2*time.Minute,
		"PT0.04S":     40 * time.Millisecond,
	} {
		if got, err := parseISODuration(in); err != nil || got != want {
			t.Errorf("%s = %v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "10S", "PT", "PTxS", "P1Y", "PT-1S"} {
		if _, err := parseISODuration(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestFormatInt(t *testing.T) {
	for _, c := range []struct {
		n      int64
		f, out string
	}{{7, "05d", "00007"}, {123456, "05d", "123456"}, {7, "", "7"}, {7, "x", "7"}} {
		if got := formatInt(c.n, c.f); got != c.out {
			t.Errorf("formatInt(%d, %q) = %q", c.n, c.f, got)
		}
	}
}

// The parsers read whatever a web server sends; they must not panic.
func FuzzParseHLS(f *testing.F) {
	for _, n := range []string{"hls_master.m3u8", "hls_media.m3u8", "hls_drm.m3u8"} {
		b, _ := os.ReadFile("testdata/" + n)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseHLS(mustURL("https://example.com/x/p.m3u8"), b)
		_ = Detect("", "", b)
	})
}

func FuzzParseDASH(f *testing.F) {
	for _, n := range []string{"dash_template.mpd", "dash_timeline.mpd", "dash_list_base.mpd"} {
		b, _ := os.ReadFile("testdata/" + n)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = ParseDASH(mustURL("https://example.com/x/m.mpd"), b)
	})
}

// Regressions found in review.

func TestDASHBudgetIsManifestWide(t *testing.T) {
	// One inherited template, many empty Representations: each alone is
	// under the per-Representation cap, together they pass the global one.
	// The global cap is what bounds memory here (about 1e6 segments are
	// built before the error), so only the error is asserted, not timing.
	in := `<MPD mediaPresentationDuration="PT100000S"><Period><AdaptationSet>` +
		`<SegmentTemplate media="$Number$" duration="1"/>` +
		strings.Repeat(`<Representation id="r"/>`, 50) +
		`</AdaptationSet></Period></MPD>`
	_, err := ParseDASH(mustURL("https://h/m.mpd"), []byte(in))
	if err == nil || !strings.Contains(err.Error(), "manifest expands") {
		t.Fatalf("accepted: %v", err)
	}
	// A single oversized Representation is refused before it is built.
	one := `<MPD mediaPresentationDuration="PT100000S"><Period><AdaptationSet>` +
		`<SegmentTemplate media="$Number$" timescale="1000" duration="1"/><Representation id="r"/></AdaptationSet></Period></MPD>`
	start := time.Now()
	if _, err := ParseDASH(mustURL("https://h/m.mpd"), []byte(one)); err == nil || !strings.Contains(err.Error(), "representation expands") {
		t.Fatalf("oversized representation: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("not refused up front: %v", time.Since(start))
	}
}

func TestDASHSegmentListInherited(t *testing.T) {
	in := `<MPD mediaPresentationDuration="PT4S"><Period><AdaptationSet mimeType="video/mp4">
		<SegmentList timescale="1000" duration="2000"><Initialization sourceURL="init.mp4"/></SegmentList>
		<Representation id="r"><SegmentList><SegmentURL media="a.m4s"/><SegmentURL media="b.m4s"/></SegmentList></Representation>
		</AdaptationSet></Period></MPD>`
	m, err := ParseDASH(mustURL("https://h/v/m.mpd"), []byte(in))
	if err != nil {
		t.Fatal(err)
	}
	r := m.Periods[0].Representations[0]
	if r.Init == nil || r.Init.URL != "https://h/v/init.mp4" || len(r.Segments) != 2 || r.Segments[1].Duration != 2*time.Second {
		t.Fatalf("inherited list: %+v", r)
	}
}

func TestDASHNoMedia(t *testing.T) {
	in := `<MPD mediaPresentationDuration="PT10S"><Period><AdaptationSet><Representation id="r"/></AdaptationSet></Period></MPD>`
	if m, err := ParseDASH(mustURL("https://h/v/manifest.mpd"), []byte(in)); err == nil {
		t.Fatalf("the MPD itself became media: %+v", m.Periods[0].Representations[0].Segments)
	}
}

func TestDASHOpenRepeat(t *testing.T) {
	tl := `<SegmentTemplate media="$Time$"><SegmentTimeline><S t="0" d="10" r="-1"/></SegmentTimeline></SegmentTemplate>`
	static := `<MPD><Period><AdaptationSet>` + tl + `<Representation id="r"/></AdaptationSet></Period></MPD>`
	if _, err := ParseDASH(mustURL("https://h/m.mpd"), []byte(static)); err == nil {
		t.Fatal("static r=-1 without an end was truncated silently")
	}
	live := `<MPD type="dynamic"><Period><AdaptationSet>` + tl + `<Representation id="r"/></AdaptationSet></Period></MPD>`
	m, err := ParseDASH(mustURL("https://h/m.mpd"), []byte(live))
	if err != nil || !m.Live || len(m.Periods[0].Representations[0].Segments) != 1 {
		t.Fatalf("live snapshot: %v %+v", err, m)
	}
}

func TestBadNumbers(t *testing.T) {
	if d, err := parseISODuration("P0Y0M0DT0H3M30.000S"); err != nil || d != 3*time.Minute+30*time.Second {
		t.Fatalf("zero years/months: %v %v", d, err)
	}
	for _, bad := range []string{"PTNaNS", "PT1e30S", "PTInfS", "P1M"} {
		if _, err := parseISODuration(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	if p, err := ParseHLS(nil, []byte("#EXTM3U\n#EXTINF:-1,Radio\nhttp://r/stream\n")); err != nil || p.Media.Segments[0].Duration != 0 {
		t.Errorf("EXTINF -1: %v", err)
	}
	for _, inf := range []string{"NaN", "1e300", "Inf"} {
		if _, err := ParseHLS(nil, []byte("#EXTM3U\n#EXTINF:"+inf+",\na.ts\n")); err == nil {
			t.Errorf("EXTINF %s accepted", inf)
		}
	}
	over := "#EXTM3U\n#EXT-X-BYTERANGE:9223372036854775807@9223372036854775807\n#EXTINF:1,\na.ts\n" +
		"#EXT-X-BYTERANGE:10\n#EXTINF:1,\na.ts\n"
	if _, err := ParseHLS(nil, []byte(over)); err == nil {
		t.Error("overflowing HLS byte range accepted")
	}
	if _, err := parseDASHRange("0-9223372036854775807"); err == nil {
		t.Error("overflowing DASH range accepted")
	}
}

func TestHLSKeyRules(t *testing.T) {
	if _, err := ParseHLS(nil, []byte("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128\n#EXTINF:1,\na.ts\n")); err == nil {
		t.Error("AES-128 key without URI accepted")
	}
	// AES-128, then a FairPlay key: the later segment is protected, not
	// decrypted with the old key.
	in := "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXT-X-KEY:METHOD=AES-128,URI=\"k\"\n#EXTINF:1,\na.ts\n" +
		`#EXT-X-KEY:METHOD=SAMPLE-AES,URI="skd://x",KEYFORMAT="com.apple.streamingkeydelivery"` + "\n#EXTINF:1,\nb.ts\n"
	p, err := ParseHLS(nil, []byte(in))
	if err != nil {
		t.Fatal(err)
	}
	if s := p.Media.Segments; s[0].Key == nil || s[1].Key != nil || !p.Media.Protected {
		t.Fatalf("method switch: %+v %+v protected=%v", s[0].Key, s[1].Key, p.Media.Protected)
	}
	// Leading blank lines: Detect and ParseHLS agree.
	body := []byte("\n\r\n#EXTM3U\n#EXTINF:1,\na.ts\n")
	if Detect("", "", body) != KindHLS {
		t.Fatal("Detect")
	}
	if _, err := ParseHLS(nil, body); err != nil {
		t.Fatalf("ParseHLS: %v", err)
	}
}

func TestReviewRound2(t *testing.T) {
	// A long VOD with several qualities is valid, though over the old cap.
	long := `<MPD mediaPresentationDuration="PT24H"><Period><AdaptationSet mimeType="video/mp4">` +
		`<SegmentTemplate media="$Number$.m4s" duration="2"/>` +
		strings.Repeat(`<Representation id="r"/>`, 5) + `</AdaptationSet></Period></MPD>`
	m, err := ParseDASH(mustURL("https://h/m.mpd"), []byte(long))
	if err != nil || len(m.Periods[0].Representations[4].Segments) != 43200 {
		t.Fatalf("long VOD: %v", err)
	}
	if _, err := parseISODuration("PT9223372036.854775807S"); err == nil {
		t.Error("duration at the int64 boundary accepted")
	}
	mapOver := "#EXTM3U\n#EXT-X-MAP:URI=\"i.mp4\",BYTERANGE=\"9223372036854775807@9223372036854775807\"\n#EXTINF:1,\na.ts\n"
	if _, err := ParseHLS(nil, []byte(mapOver)); err == nil {
		t.Error("overflowing EXT-X-MAP range accepted")
	}
	for name, tmpl := range map[string]string{
		"number":   `<SegmentTemplate media="$Number$" timescale="9000000000000000000" duration="1"/>`,
		"timeline": `<SegmentTemplate media="$Time$" timescale="9000000000000000000"><SegmentTimeline><S t="0" d="1" r="-1"/></SegmentTimeline></SegmentTemplate>`,
	} {
		in := `<MPD mediaPresentationDuration="PT10S"><Period><AdaptationSet>` + tmpl + `<Representation id="r"/></AdaptationSet></Period></MPD>`
		if _, err := ParseDASH(mustURL("https://h/m.mpd"), []byte(in)); err == nil {
			t.Errorf("huge timescale (%s) accepted", name)
		}
	}
}
