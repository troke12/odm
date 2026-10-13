package engine

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/publicsuffix"

	"odm/internal/ffmpeg"
	"odm/internal/media"
)

// Segmented downloads (HLS, DASH).
//
// A stream is fetched as many small pieces instead of one file, so the
// byte-range machinery in download.go does not apply. Design:
//   - Every segment is written to its own file in a work folder next to
//     the target ("<name>.odmparts/t<track>/<n>.seg"), under a temporary
//     name renamed when complete. Resuming is therefore "skip the files
//     that exist"; nothing per segment has to be persisted, and a crash
//     loses at most the segments in flight.
//   - The segment list is not persisted either. Segment URLs are usually
//     signed and expire, so every run fetches the manifest again and
//     rebuilds the list; the segment count per track is kept to detect a
//     manifest that changed underneath (resuming would then mix streams).
//   - When every segment is present they are concatenated in order, with
//     the init section in front (again whenever it changes). One track is
//     concatenated straight into the .odmpart; a separate video and audio
//     track are each concatenated, then joined by ffmpeg.
//   - A 403 or 410 on a segment usually means its signed URL expired: the
//     manifest is fetched again (once per run) and the segment retried.
//
// Gotchas:
//   - Live playlists and DRM-protected streams are refused with a clear
//     error rather than producing a partial or unplayable file.
//   - A DASH SegmentBase track is one whole file and downloads over a
//     single connection; only segmented tracks are parallel.
//   - AES-128 segments are decrypted in memory once complete; HLS segments
//     are a few MB, so this stays small.

// StreamJob is the persisted description of a segmented download.
type StreamJob struct {
	Kind     media.Kind `json:"kind"`     // media.KindHLS or media.KindDASH
	Manifest string     `json:"manifest"` // the URL the stream was found at
	// HLS: the media playlists to fetch. Empty Video means "pick the best
	// variant of Manifest"; Audio is set when the audio is a separate
	// rendition.
	Video string `json:"video,omitempty"`
	Audio string `json:"audio,omitempty"`
	// Variant and AudioKey pin the HLS rendition chosen from a master
	// playlist (see hlsVariantKey), so a resume cannot switch to another
	// one whose URLs differ only by their signature.
	Variant  string `json:"variant,omitempty"`
	AudioKey string `json:"audio_key,omitempty"`
	// Live marks a live HLS playlist, recorded until stopped (live.go).
	// It is set when the playlist is first read as live. Recording is set
	// once the recording has claimed its file name, so a later run
	// continues it.
	Live      bool `json:"live,omitempty"`
	Recording bool `json:"recording,omitempty"`
	// WorkName is the file name the work folder was made under, when the
	// file was renamed since (kept as .ts instead of .mp4, see keepTS).
	WorkName string `json:"work_name,omitempty"`
	// DASH: the Representation IDs. Empty means "pick the best"; the pick
	// is written back here on the first run.
	VideoRep string `json:"video_rep,omitempty"`
	AudioRep string `json:"audio_rep,omitempty"`
	// Done is the bytes on disk when the state was saved, shown after a
	// restart until the next run counts the segment files again.
	Done int64 `json:"done,omitempty"`
	// Counts holds the segment count per track seen on the first run.
	Counts []int `json:"counts,omitempty"`
	// Estimate is the expected size in bytes (bandwidth × duration), shown
	// until enough segments are done to extrapolate; 0 when unknown.
	Estimate int64 `json:"estimate,omitempty"`
}

// ffmpegSetting is the configured ffmpeg path (Settings.FFmpegPath). Like
// the user agent it is read from download goroutines, hence atomic.
var ffmpegSetting atomic.Pointer[string]

func setFFmpegPath(p string) { ffmpegSetting.Store(&p) }

func ffmpegConfigured() string {
	if p := ffmpegSetting.Load(); p != nil {
		return *p
	}
	return ""
}

// piece is one resource to fetch: a segment or an init section.
type piece struct {
	url string
	rng *media.ByteRange
	key *media.HLSKey
	seq int64
	dur time.Duration // playing time; set for live segments only
}

// streamTrack is one elementary stream (video, audio, or both muxed).
type streamTrack struct {
	ext     string // container of the concatenated track: ts, mp4, m4a, webm
	video   bool
	inits   []piece
	segs    []piece
	segInit []int // index into inits per segment, -1 for none
	live    bool  // a recording: a skipped segment has no file
}

// permanentError is a refusal that retrying the same request cannot fix
// (404 and most other 4xx); the run fails at once instead of backing off.
type permanentError struct{ status string }

func (e permanentError) Error() string { return "segment: " + e.status }

// errExpired marks a refusal that a fresh manifest may cure.
var errExpired = errors.New("the server refused a segment (expired link?)")

// maxManifestSize bounds manifest downloads.
const maxManifestSize = 16 << 20

// runStream is Start for a segmented download.
func (d *Download) runStream(ctx context.Context) {
	d.mu.Lock()
	live := d.Stream.Live
	d.mu.Unlock()
	var err error
	if live {
		err = d.recordLive(ctx)
	} else {
		var tracks []*streamTrack
		tracks, err = d.loadStream(ctx)
		if errors.Is(err, errLiveStream) {
			live = true
			err = d.recordLive(ctx)
		} else {
			if err == nil {
				err = d.prepareStream(tracks)
			}
			if err == nil {
				err = d.fetchStream(ctx, tracks)
			}
			if err == nil {
				err = d.assembleStream(ctx, tracks)
			}
		}
	}
	// finish may move the result to a free name; the work folder keeps
	// the name it was created under.
	work := d.workDir()
	if d.isRemoved() {
		return // being removed: nothing to finish or report
	}
	if err == nil {
		err = d.finish()
	}
	if live && err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, errRemoved) {
		// A recording assembles after it was stopped, so ctx is done
		// even when assembly itself failed: report that failure.
		d.fail(err)
		return
	}
	switch {
	case err == nil:
		_ = os.RemoveAll(work)
		d.mu.Lock()
		d.Status = StatusComplete
		if st, serr := os.Stat(filepath.Join(d.Dir, d.FileName)); serr == nil {
			d.Total, d.streamDone = st.Size(), st.Size()
		}
		d.mu.Unlock()
	case ctx.Err() != nil:
		d.mu.Lock()
		d.Status = StatusPaused
		d.mu.Unlock()
	default:
		d.fail(err)
	}
}

/* ---------- manifests ---------- */

// loadStream fetches the manifest(s) and builds the tracks to download.
func (d *Download) loadStream(ctx context.Context) ([]*streamTrack, error) {
	d.mu.Lock()
	job := *d.Stream
	d.mu.Unlock()
	switch job.Kind {
	case media.KindHLS:
		return d.loadHLS(ctx, job)
	case media.KindDASH:
		return d.loadDASH(ctx, job)
	}
	return nil, fmt.Errorf("unsupported stream type %q", job.Kind)
}

// fetchText downloads a manifest and returns it with the URL it finally
// came from (relative references resolve against that, not the original,
// when the server redirected).
func (d *Download) fetchText(ctx context.Context, raw string) ([]byte, *url.URL, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, nil, err
	}
	d.decorate(req)
	resp, err := d.guardedClient().Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("manifest: %s", resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxManifestSize+1))
	if err != nil {
		return nil, nil, err
	}
	if len(data) > maxManifestSize {
		return nil, nil, errors.New("manifest is too large")
	}
	return data, resp.Request.URL, nil
}

// decorate adds the download's headers, cookies and credentials.
//
// The cookies and any Authorization header came with the page the stream
// was found on. A playlist may name segments or keys on any host, so they
// go only to hosts of the same site (same registrable domain) as the
// manifest; anything else would hand the user's session to whoever wrote
// the playlist. Go strips Cookie on cross-domain redirects, but these are
// fresh requests, so that protection does not apply here.
func (d *Download) decorate(req *http.Request) {
	d.mu.Lock()
	headers, cookies, auth := d.Headers, d.cookies, d.auth
	manifest := d.URL
	if d.Stream != nil {
		manifest = d.Stream.Manifest
	}
	d.mu.Unlock()
	if !sameSite(manifest, req.URL) {
		safe := make(map[string]string, len(headers))
		for k, v := range headers {
			if !strings.EqualFold(k, "Cookie") && !strings.EqualFold(k, "Authorization") {
				safe[k] = v
			}
		}
		headers, cookies = safe, nil
	}
	applyHeaders(req, headers)
	if len(cookies) > 0 && req.Header.Get("Cookie") == "" {
		req.Header.Set("Cookie", cookieHeader(cookies))
	}
	if auth != nil {
		if user, pass, ok := auth(req.URL.Hostname()); ok {
			req.SetBasicAuth(user, pass)
		}
	}
}

// guardManifestLocked is the address the local-network guard judges a
// download by: the address it was given (a stream's manifest, else its
// URL, or where it was trusted from). The caller holds d.mu.
func (d *Download) guardManifestLocked() string {
	manifest := d.URL
	if d.TrustFrom != "" {
		manifest = d.TrustFrom
	}
	if d.Stream != nil && d.Stream.Manifest != "" {
		manifest = d.Stream.Manifest
	}
	return manifest
}

// guardedClient is the client for every request of a download, stream or
// file: the download's own, kept off the local network when the address it
// was given (a stream's manifest, else its URL) is public (see localGuard).
func (d *Download) guardedClient() *http.Client {
	d.mu.Lock()
	defer d.mu.Unlock()
	manifest := d.guardManifestLocked()
	if d.guarded == nil || d.guardedFrom != d.client || d.guardedFor != manifest {
		if d.guarded != nil {
			d.guarded.CloseIdleConnections() // its pool is its own
		}
		d.guarded, d.guardedFrom, d.guardedFor = guardLocal(d.client, manifest), d.client, manifest
	}
	return d.guarded
}

// sameSite reports whether u belongs to the site (registrable domain, e.g.
// example.co.uk) of the manifest URL. IP addresses and single-label hosts
// must match exactly.
func sameSite(manifest string, u *url.URL) bool {
	m, err := url.Parse(manifest)
	if err != nil {
		return false
	}
	a, b := strings.ToLower(m.Hostname()), strings.ToLower(u.Hostname())
	if a == b {
		return true
	}
	// publicsuffix would read an IPv4 address's last octet as a TLD, so
	// 10.0.0.1 and 192.168.0.1 would both become "0.1".
	if net.ParseIP(a) != nil || net.ParseIP(b) != nil {
		return false
	}
	sa, err1 := publicsuffix.EffectiveTLDPlusOne(a)
	sb, err2 := publicsuffix.EffectiveTLDPlusOne(b)
	return err1 == nil && err2 == nil && sa == sb
}

func (d *Download) loadHLS(ctx context.Context, job StreamJob) ([]*streamTrack, error) {
	videoURL, audioURL := job.Video, job.Audio
	// When Manifest is already a media playlist, keep its parse instead of
	// fetching it a second time.
	var direct *media.HLSMedia
	if videoURL == "" {
		data, base, err := d.fetchText(ctx, job.Manifest)
		if err != nil {
			return nil, err
		}
		p, err := media.ParseHLS(base, data)
		if err != nil {
			return nil, err
		}
		if p.Master != nil {
			v, a, err := pickHLS(p.Master, job.Variant, job.AudioKey)
			if err != nil {
				return nil, err
			}
			videoURL, audioURL = v.URL, ""
			if a != nil {
				audioURL = a.URL
			}
			d.mu.Lock()
			if d.Stream.Variant == "" {
				d.Stream.Variant = hlsVariantKey(*v)
				if a != nil {
					d.Stream.AudioKey = hlsAudioKey(*a)
				}
			}
			d.mu.Unlock()
		} else {
			videoURL, direct = base.String(), p.Media
		}
	}
	var tracks []*streamTrack
	for i, u := range []string{videoURL, audioURL} {
		if u == "" {
			continue
		}
		m := direct
		if i > 0 || m == nil {
			data, base, err := d.fetchText(ctx, u)
			if err != nil {
				return nil, err
			}
			p, err := media.ParseHLS(base, data)
			if err != nil {
				return nil, err
			}
			if p.Media == nil {
				return nil, errors.New("expected a media playlist, got a master playlist")
			}
			m = p.Media
		}
		switch {
		case m.Protected:
			return nil, errors.New("this stream is DRM-protected and cannot be downloaded")
		case m.Live():
			return nil, errLiveStream
		case len(m.Segments) == 0:
			return nil, errors.New("the playlist has no segments")
		}
		t := &streamTrack{video: i == 0, ext: "ts"}
		maps := map[*media.HLSMap]int{}
		for _, s := range m.Segments {
			idx := -1
			if s.Map != nil {
				n, ok := maps[s.Map]
				if !ok {
					n = len(t.inits)
					maps[s.Map] = n
					t.inits = append(t.inits, piece{url: s.Map.URL, rng: s.Map.Range, key: s.Map.Key})
				}
				idx = n
			}
			t.segs = append(t.segs, piece{url: s.URL, rng: s.Range, key: s.Key, seq: s.Sequence})
			t.segInit = append(t.segInit, idx)
		}
		// An init section means fragmented MP4; otherwise MPEG-TS.
		if len(t.inits) > 0 {
			t.ext = "mp4"
			if i == 1 {
				t.ext = "m4a"
			}
		} else if i == 1 && strings.EqualFold(path.Ext(urlPath(m.Segments[0].URL)), ".aac") {
			t.ext = "aac"
		}
		tracks = append(tracks, t)
	}
	return tracks, nil
}

func urlPath(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return u.Path
	}
	return raw
}

// hlsVariantKey identifies a variant across reloads of a master playlist
// without its URL query (signatures change, the rendition does not).
func hlsVariantKey(v media.HLSVariant) string {
	return fmt.Sprintf("%d|%dx%d|%s|%s", v.Bandwidth, v.Width, v.Height, v.Codecs, urlPath(v.URL))
}

func hlsAudioKey(r media.HLSRendition) string {
	return r.GroupID + "|" + r.Name + "|" + r.Language + "|" + urlPath(r.URL)
}

// pickHLS returns the pinned variant (and audio) when keys are given,
// else the best one. A pinned rendition that is gone is an error: picking
// another would mix two encodings in one file.
//
// Matching is exact first, then without the URL path when exactly one
// candidate fits: some servers put a session ID in the playlist path
// (chunklist_w<session>.m3u8) and change it on every master fetch.
func pickHLS(m *media.HLSMaster, variant, audio string) (*media.HLSVariant, *media.HLSRendition, error) {
	if variant == "" {
		v, a, ok := bestHLS(m)
		if !ok {
			return nil, nil, errors.New("the playlist lists no video")
		}
		return v, a, nil
	}
	gone := errors.New("the chosen quality is no longer offered; remove the download and add it again")
	v := matchKey(len(m.Variants), func(i int) string { return hlsVariantKey(m.Variants[i]) }, variant)
	if v < 0 {
		return nil, nil, gone
	}
	if audio == "" {
		return &m.Variants[v], nil, nil
	}
	a := matchKey(len(m.Renditions), func(i int) string { return hlsAudioKey(m.Renditions[i]) }, audio)
	if a < 0 {
		return nil, nil, gone
	}
	return &m.Variants[v], &m.Renditions[a], nil
}

// matchKey finds want among n keys: exactly, else by everything before
// the last "|" (the URL path) when that is unambiguous. -1 if neither.
func matchKey(n int, key func(int) string, want string) int {
	loose := func(k string) string {
		if i := strings.LastIndexByte(k, '|'); i >= 0 {
			return k[:i]
		}
		return k
	}
	found := -1
	for i := 0; i < n; i++ {
		k := key(i)
		if k == want {
			return i
		}
		if loose(k) == loose(want) {
			if found >= 0 {
				found = -2 // ambiguous
			} else if found == -1 {
				found = i
			}
		}
	}
	if found < 0 {
		return -1
	}
	return found
}

// BestHLS picks the highest-bandwidth variant and, when its audio is a
// separate rendition, the default (else first) rendition of its group.
func BestHLS(m *media.HLSMaster) (video, audio string, ok bool) {
	v, a, ok := bestHLS(m)
	if !ok {
		return "", "", false
	}
	if a != nil {
		audio = a.URL
	}
	return v.URL, audio, true
}

func bestHLS(m *media.HLSMaster) (*media.HLSVariant, *media.HLSRendition, bool) {
	var best *media.HLSVariant
	for i := range m.Variants {
		v := &m.Variants[i]
		if best == nil || v.Bandwidth > best.Bandwidth {
			best = v
		}
	}
	if best == nil {
		return nil, nil, false
	}
	return best, HLSAudioFor(m, best.Audio), true
}

// HLSAudioFor returns the audio rendition to pair with a variant of the
// given group: the default one, else the first. nil means the audio is
// muxed into the variant, including when the default rendition has no URI:
// the others are then alternates (commentary, another language), and
// picking one would replace the main audio. hlsAudios offers the same
// default as "In the video".
func HLSAudioFor(m *media.HLSMaster, group string) *media.HLSRendition {
	if group == "" {
		return nil
	}
	var first *media.HLSRendition
	for i := range m.Renditions {
		r := &m.Renditions[i]
		if r.Type != "AUDIO" || r.GroupID != group {
			continue
		}
		if r.URL == "" {
			if r.Default {
				return nil
			}
			continue
		}
		if r.Default {
			return r
		}
		if first == nil {
			first = r
		}
	}
	return first
}

func (d *Download) loadDASH(ctx context.Context, job StreamJob) ([]*streamTrack, error) {
	data, base, err := d.fetchText(ctx, job.Manifest)
	if err != nil {
		return nil, err
	}
	m, err := media.ParseDASH(base, data)
	if err != nil {
		return nil, err
	}
	if m.Live {
		return nil, errors.New("live streams are not supported yet")
	}
	videoID, audioID := job.VideoRep, job.AudioRep
	if videoID == "" && audioID == "" {
		videoID, audioID = BestDASH(m)
		d.mu.Lock()
		if d.Stream.VideoRep == "" && d.Stream.AudioRep == "" {
			d.Stream.VideoRep, d.Stream.AudioRep = videoID, audioID
		}
		d.mu.Unlock()
	}
	var tracks []*streamTrack
	for i, id := range []string{videoID, audioID} {
		if id == "" {
			continue
		}
		t := &streamTrack{video: i == 0}
		for _, p := range m.Periods {
			r := pickRep(p, id, i == 0)
			if r == nil {
				return nil, fmt.Errorf("track %q is missing from the manifest", id)
			}
			if r.Protected {
				return nil, errors.New("this stream is DRM-protected and cannot be downloaded")
			}
			idx := -1
			if r.Init != nil {
				idx = len(t.inits)
				t.inits = append(t.inits, piece{url: r.Init.URL, rng: r.Init.Range})
			}
			for _, s := range r.Segments {
				t.segs = append(t.segs, piece{url: s.URL, rng: s.Range})
				t.segInit = append(t.segInit, idx)
			}
			if t.ext == "" {
				t.ext = dashExt(r)
			}
		}
		if len(t.segs) == 0 {
			return nil, fmt.Errorf("track %q has no segments", id)
		}
		tracks = append(tracks, t)
	}
	if len(tracks) == 0 {
		return nil, errors.New("the manifest lists no audio or video")
	}
	return tracks, nil
}

// pickRep finds the Representation with the given ID in a Period; across
// periods (ads, chapters) IDs often differ, so it falls back to the
// highest-bandwidth track of the same kind.
func pickRep(p media.DASHPeriod, id string, video bool) *media.DASHRepresentation {
	var best *media.DASHRepresentation
	for i := range p.Representations {
		r := &p.Representations[i]
		if r.ID == id {
			return r
		}
		if r.IsVideo() == video && (r.IsVideo() || r.IsAudio()) && (best == nil || r.Bandwidth > best.Bandwidth) {
			best = r
		}
	}
	return best
}

// BestDASH picks the highest-bandwidth video and audio of the first
// period. A manifest without separate audio returns "" for it.
func BestDASH(m *media.DASHManifest) (video, audio string) {
	if len(m.Periods) == 0 {
		return "", ""
	}
	var bv, ba *media.DASHRepresentation
	for i := range m.Periods[0].Representations {
		r := &m.Periods[0].Representations[i]
		switch {
		case r.IsVideo() && (bv == nil || r.Bandwidth > bv.Bandwidth):
			bv = r
		case r.IsAudio() && (ba == nil || r.Bandwidth > ba.Bandwidth):
			ba = r
		}
	}
	if bv != nil {
		video = bv.ID
	}
	if ba != nil {
		audio = ba.ID
	}
	return video, audio
}

func dashExt(r *media.DASHRepresentation) string {
	switch {
	case strings.Contains(r.MimeType, "webm"):
		return "webm"
	case r.IsAudio():
		return "m4a"
	}
	return "mp4"
}

/* ---------- naming and progress ---------- */

// outputExt is the container of the finished file.
func outputExt(tracks []*streamTrack) string {
	if len(tracks) == 1 {
		// An MPEG-TS track is remuxed into MP4 when ffmpeg is there (see
		// remuxTS); without it the file stays MPEG-TS.
		if tracks[0].ext == "ts" {
			if _, err := ffmpeg.Locate(ffmpegConfigured()); err == nil {
				return "mp4"
			}
		}
		return tracks[0].ext
	}
	// Two tracks are joined by ffmpeg: WebM when both are WebM, else MP4
	// (MPEG-TS video with AAC audio remuxes into MP4 without re-encoding).
	if tracks[0].ext == "webm" && tracks[1].ext == "webm" {
		return "webm"
	}
	return "mp4"
}

// streamName gives a page-title name the right extension, replacing a
// manifest one (".m3u8", ".mpd") taken from the URL.
func streamName(name, ext string) string {
	name = strings.TrimSpace(name)
	switch strings.ToLower(filepath.Ext(name)) {
	case ".m3u8", ".m3u", ".mpd", ".ts", ".mp4", ".m4a", ".webm", ".aac", ".m4s":
		name = strings.TrimSuffix(name, filepath.Ext(name))
	}
	if name == "" {
		name = "video"
	}
	return name + "." + ext
}

// prepareStream checks the segment counts, names the target and counts
// what an earlier run already fetched.
func (d *Download) prepareStream(tracks []*streamTrack) error {
	counts := make([]int, len(tracks))
	for i, t := range tracks {
		counts[i] = len(t.segs)
	}
	ext := outputExt(tracks) // looks for ffmpeg: not under d.mu
	d.mu.Lock()
	prev := d.Stream.Counts
	fresh := len(prev) == 0
	if !fresh && !equalInts(prev, counts) {
		d.mu.Unlock()
		return errors.New("the stream changed since this download started; remove it and add it again")
	}
	if fresh {
		d.FileName = streamName(d.FileName, ext)
	}
	d.mu.Unlock()

	if err := d.openTarget(fresh); err != nil {
		return err
	}
	if fresh {
		// The name was just claimed, so any work folder under it is a
		// leftover of an unrelated download: its segments must not be
		// taken for this stream's.
		if err := os.RemoveAll(d.workDir()); err != nil {
			return err
		}
	}
	d.mu.Lock()
	// Only now: a failed claim must leave the next run "fresh", so it
	// claims a name with O_EXCL instead of reusing someone else's.
	d.Stream.Counts = counts
	d.RangeOK = true // resumable, segment by segment
	d.mu.Unlock()

	var done int64
	have, total := 0, 0
	for ti, t := range tracks {
		for i := range t.inits {
			if p := d.piecePath(ti, "init"+strconv.Itoa(i)); havePiece(p, &t.inits[i]) {
				done += fileSize(p)
			}
		}
		for i := range t.segs {
			total++
			if p := d.piecePath(ti, strconv.Itoa(i)); havePiece(p, &t.segs[i]) {
				done += fileSize(p)
				have++
			}
		}
	}
	d.mu.Lock()
	d.streamDone, d.streamHave, d.streamSegs = done, have, total
	d.mu.Unlock()
	d.updateEstimate()
	return nil
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// havePiece reports whether a piece's file is complete. Empty files are
// treated as missing (a crash between create and write leaves one); a
// plain byte-range piece must have exactly its length. Decrypted pieces
// lose their padding, so only non-empty is checked for them.
func havePiece(path string, p *piece) bool {
	n := fileSize(path)
	if n <= 0 {
		return false
	}
	if p.rng != nil && p.key == nil {
		return n == p.rng.Length
	}
	return true
}

func fileSize(p string) int64 {
	if st, err := os.Stat(p); err == nil {
		return st.Size()
	}
	return 0
}

// workDir holds the segments of a stream until they are assembled.
func (d *Download) workDir() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Stream != nil && d.Stream.WorkName != "" {
		return filepath.Join(d.Dir, d.Stream.WorkName+".odmparts")
	}
	return filepath.Join(d.Dir, d.FileName+".odmparts")
}

func (d *Download) piecePath(track int, name string) string {
	return filepath.Join(d.workDir(), "t"+strconv.Itoa(track), name+".seg")
}

// updateEstimate extrapolates the total from the segments done so far,
// falling back to the job's estimate before enough are in.
func (d *Download) updateEstimate() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Stream != nil && d.Stream.Live {
		d.Total = d.streamDone // a recording has no end to estimate
		return
	}
	switch {
	case d.streamHave >= 3:
		d.Total = d.streamDone * int64(d.streamSegs) / int64(d.streamHave)
	case d.Stream.Estimate > 0:
		d.Total = d.Stream.Estimate
	}
	if d.Total < d.streamDone {
		d.Total = d.streamDone
	}
}

/* ---------- fetching ---------- */

type pieceJob struct {
	track int
	name  string // file name in the work folder
	p     *piece
	init  bool
}

// fetchStream downloads every missing piece with Connections workers.
func (d *Download) fetchStream(ctx context.Context, tracks []*streamTrack) error {
	var jobs []pieceJob
	for ti, t := range tracks {
		for i := range t.inits {
			name := "init" + strconv.Itoa(i)
			if !havePiece(d.piecePath(ti, name), &t.inits[i]) {
				jobs = append(jobs, pieceJob{track: ti, name: name, p: &t.inits[i], init: true})
			}
		}
		for i := range t.segs {
			name := strconv.Itoa(i)
			if !havePiece(d.piecePath(ti, name), &t.segs[i]) {
				jobs = append(jobs, pieceJob{track: ti, name: name, p: &t.segs[i]})
			}
		}
		if err := os.MkdirAll(filepath.Dir(d.piecePath(ti, "x")), 0o755); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s := &streamRun{d: d, tracks: tracks, keys: map[string][]byte{}}
	queue := make(chan pieceJob)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once

	workers := d.Connections
	if workers < 1 {
		workers = 1
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range queue {
				if err := s.fetchWithRetry(ctx, j); err != nil {
					errOnce.Do(func() { firstErr = err; cancel() })
					continue
				}
				if !j.init {
					d.mu.Lock()
					d.streamHave++
					d.mu.Unlock()
				}
				d.updateEstimate()
				d.notify(false)
			}
		}()
	}
feed:
	for _, j := range jobs {
		select {
		case queue <- j:
		case <-ctx.Done():
			break feed
		}
	}
	close(queue)
	wg.Wait()
	d.updateEstimate()
	if firstErr != nil && !errors.Is(firstErr, context.Canceled) {
		return firstErr
	}
	return ctx.Err()
}

// streamRun is the state shared by the workers of one run.
type streamRun struct {
	d      *Download
	tracks []*streamTrack

	mu        sync.Mutex
	keys      map[string][]byte
	refreshed bool // a reload was attempted this run
	refreshOK bool // and it produced usable URLs
}

func (s *streamRun) fetchWithRetry(ctx context.Context, j pieceJob) error {
	var err error
	backoff := 300 * time.Millisecond
	for attempt := 0; attempt < MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = s.fetch(ctx, j)
		if err == nil || ctx.Err() != nil {
			return err
		}
		var perm permanentError
		if errors.As(err, &perm) || errors.Is(err, errLocalNetwork) {
			return err
		}
		if errors.Is(err, errExpired) {
			if !s.refresh(ctx) {
				return err
			}
			continue // retry at once with the fresh URL
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
	return err
}

// refresh reloads the manifest once per run and swaps in the new URLs.
// It reports whether a retry is worthwhile.
func (s *streamRun) refresh(ctx context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refreshed {
		// Another worker already reloaded: retry only if that worked,
		// otherwise every worker would hammer the same expired URLs.
		return s.refreshOK
	}
	s.refreshed = true
	fresh, err := s.d.loadStream(ctx)
	if err != nil || len(fresh) != len(s.tracks) {
		return false
	}
	// Check every track before swapping any, so a mismatch cannot leave
	// some tracks on new URLs and others on old ones.
	for i, t := range s.tracks {
		f := fresh[i]
		if len(f.segs) != len(t.segs) || len(f.inits) != len(t.inits) {
			return false
		}
	}
	s.d.mu.Lock()
	defer s.d.mu.Unlock()
	for i, t := range s.tracks {
		f := fresh[i]
		for k := range t.segs {
			t.segs[k].url = f.segs[k].url
			t.segs[k].key = f.segs[k].key
		}
		for k := range t.inits {
			t.inits[k].url = f.inits[k].url
			t.inits[k].key = f.inits[k].key
		}
	}
	s.refreshOK = true
	return true
}

// fetch downloads one piece into its file, decrypting it when needed.
func (s *streamRun) fetch(ctx context.Context, j pieceJob) error {
	d := s.d
	d.mu.Lock()
	p := *j.p // the URL may be swapped by refresh
	d.mu.Unlock()
	if p.key != nil {
		// Fetch the key first: if it is refused, the segment need not be
		// downloaded again on every retry.
		if _, err := s.key(ctx, p.key.URL); err != nil {
			return err
		}
		if j.init && len(p.key.IV) != 16 {
			return errors.New("encrypted init section without an IV (invalid playlist)")
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return err
	}
	d.decorate(req)
	if p.rng != nil {
		req.Header.Set("Range", p.rng.Header())
	}
	resp, err := d.guardedClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone:
		return fmt.Errorf("%w: %s", errExpired, resp.Status)
	case p.rng != nil && resp.StatusCode == http.StatusPartialContent:
		if cs, _, _, ok := ParseContentRange(resp.Header.Get("Content-Range")); ok && cs != p.rng.Offset {
			return fmt.Errorf("range mismatch: asked %d, got %d", p.rng.Offset, cs)
		}
	case p.rng != nil && resp.StatusCode == http.StatusOK && p.rng.Offset == 0:
		// The server ignored Range but the piece starts the file: read
		// just its length below.
	case resp.StatusCode >= 400 && resp.StatusCode < 500 &&
		resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusTooManyRequests:
		return permanentError{resp.Status}
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("segment: %s", resp.Status)
	}
	var body io.Reader = resp.Body
	if p.rng != nil {
		body = io.LimitReader(resp.Body, p.rng.Length)
	}

	final := d.piecePath(j.track, j.name)
	tmp := final + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	n, err := d.copyCounted(ctx, f, body)
	if err == nil {
		// Durable before the rename: after a power loss a renamed but
		// empty file would pass for a finished segment.
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && p.rng != nil && n != p.rng.Length {
		err = fmt.Errorf("short segment: %d of %d bytes", n, p.rng.Length)
	}
	if err == nil && p.key != nil {
		err = s.decrypt(ctx, tmp, &p)
	}
	if err == nil {
		err = os.Rename(tmp, final)
	}
	if err != nil {
		_ = os.Remove(tmp)
		d.mu.Lock()
		d.streamDone -= n // counted while streaming; not kept
		d.mu.Unlock()
		return err
	}
	if p.key != nil { // decryption removed the padding: count the real size
		d.mu.Lock()
		d.streamDone += fileSize(final) - n
		d.mu.Unlock()
	}
	return nil
}

// copyCounted copies r to w, counting progress and honouring the limiter.
func (d *Download) copyCounted(ctx context.Context, w io.Writer, r io.Reader) (int64, error) {
	buf := make([]byte, 64*1024)
	var total int64
	for {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		n, rerr := r.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
			d.mu.Lock()
			d.streamDone += int64(n)
			lim := d.limiter
			d.mu.Unlock()
			d.notify(false)
			if werr := lim.Wait(ctx, n); werr != nil {
				return total, werr
			}
		}
		if rerr == io.EOF {
			return total, nil
		}
		if rerr != nil {
			return total, rerr
		}
	}
}

// decrypt replaces the file with its AES-128-CBC plaintext.
func (s *streamRun) decrypt(ctx context.Context, file string, p *piece) error {
	key, err := s.key(ctx, p.key.URL)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data)%aes.BlockSize != 0 {
		return errors.New("encrypted segment has a broken length")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	cipher.NewCBCDecrypter(block, p.key.IVFor(p.seq)).CryptBlocks(data, data)
	// PKCS#7 padding.
	pad := int(data[len(data)-1])
	if pad == 0 || pad > aes.BlockSize || pad > len(data) {
		return errors.New("wrong decryption key or broken segment")
	}
	for _, b := range data[len(data)-pad:] {
		if int(b) != pad {
			return errors.New("wrong decryption key or broken segment")
		}
	}
	return os.WriteFile(file, data[:len(data)-pad], 0o644)
}

// key fetches (once) a 16-byte AES key.
func (s *streamRun) key(ctx context.Context, keyURL string) ([]byte, error) {
	s.mu.Lock()
	k, ok := s.keys[keyURL]
	s.mu.Unlock()
	if ok {
		return k, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, keyURL, nil)
	if err != nil {
		return nil, err
	}
	s.d.decorate(req)
	resp, err := s.d.guardedClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusForbidden, http.StatusGone:
		return nil, fmt.Errorf("%w: decryption key %s", errExpired, resp.Status)
	default:
		return nil, fmt.Errorf("decryption key: %s", resp.Status)
	}
	k, err = io.ReadAll(io.LimitReader(resp.Body, 17))
	if err != nil {
		return nil, err
	}
	if len(k) != 16 {
		return nil, fmt.Errorf("decryption key has %d bytes, want 16", len(k))
	}
	s.mu.Lock()
	s.keys[keyURL] = k
	s.mu.Unlock()
	return k, nil
}

/* ---------- assembly ---------- */

// assembleStream concatenates the pieces into the .odmpart, joining two
// tracks with ffmpeg.
func (d *Download) assembleStream(ctx context.Context, tracks []*streamTrack) error {
	d.mu.Lock()
	out := d.file
	d.file = nil
	target := filepath.Join(d.Dir, d.FileName) + ".odmpart"
	ext := filepath.Ext(d.FileName)
	d.mu.Unlock()
	if out == nil {
		var err error
		if out, err = os.OpenFile(target, os.O_CREATE|os.O_RDWR, 0o644); err != nil {
			return err
		}
	}
	// An earlier assembly may have failed half way: start from empty.
	if err := out.Truncate(0); err != nil {
		_ = out.Close()
		return err
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		_ = out.Close()
		return err
	}

	if len(tracks) == 1 && tracks[0].ext == "ts" && strings.EqualFold(ext, ".mp4") {
		_ = out.Close()
		return d.remuxTS(ctx, target, tracks[0])
	}
	if len(tracks) == 1 {
		err := d.concat(ctx, out, 0, tracks[0])
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		return err
	}
	_ = out.Close()

	bin, err := ffmpeg.Locate(ffmpegConfigured())
	if err != nil {
		// The segments stay; resuming after installing ffmpeg only redoes
		// the assembly.
		return fmt.Errorf("video and audio are separate and need ffmpeg to be joined: %w", err)
	}
	var parts []string
	for ti, t := range tracks {
		p := filepath.Join(d.workDir(), "track"+strconv.Itoa(ti)+"."+t.ext)
		f, err := os.Create(p)
		if err != nil {
			return err
		}
		err = d.concat(ctx, f, ti, t)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		parts = append(parts, p)
	}
	merged := filepath.Join(d.workDir(), "merged"+ext)
	if err := ffmpeg.Merge(ctx, bin, parts[0], parts[1], merged); err != nil {
		return err
	}
	return os.Rename(merged, target)
}

// remuxTS joins an MPEG-TS track and remuxes it into the MP4 at target,
// piping the joined track into ffmpeg so it is never written to disk
// (segments plus the MP4 already take twice the recording's size). When
// ffmpeg has gone missing since the file was named, or the remux fails,
// the MPEG-TS is kept under a .ts name instead, with the reason in Err:
// losing the download over the container would be worse.
func (d *Download) remuxTS(ctx context.Context, target string, t *streamTrack) error {
	bin, why := ffmpeg.Locate(ffmpegConfigured())
	if why == nil {
		remuxed := filepath.Join(d.workDir(), "remuxed.mp4")
		pr, pw := io.Pipe()
		joined := make(chan error, 1)
		go func() {
			err := d.concat(ctx, pw, 0, t)
			pw.CloseWithError(err)
			joined <- err
		}()
		why = ffmpeg.RemuxTS(ctx, bin, pr, remuxed)
		pr.CloseWithError(errors.New("ffmpeg stopped reading")) // unblocks concat
		jerr := <-joined
		if why == nil && jerr == nil {
			return os.Rename(remuxed, target)
		}
		_ = os.Remove(remuxed)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if why == nil {
			if !errors.Is(jerr, io.ErrClosedPipe) {
				return jerr // joining failed (a missing piece, a read error)
			}
			why = jerr
		}
	}
	return d.keepTS(ctx, target, t, why)
}

// keepTS joins the MPEG-TS track under a .ts name in place of the .mp4 the
// download was claimed as. The work folder keeps the name it was made
// under (Stream.WorkName), so it is still found after a restart.
func (d *Download) keepTS(ctx context.Context, target string, t *streamTrack, why error) error {
	d.mu.Lock()
	dir, name := d.Dir, d.FileName
	d.mu.Unlock()
	tsName, f, err := claimName(dir, strings.TrimSuffix(name, filepath.Ext(name))+".ts")
	if err != nil {
		return err
	}
	err = d.concat(ctx, f, 0, t)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(filepath.Join(dir, tsName) + ".odmpart")
		return err
	}
	_ = os.Remove(target)
	d.mu.Lock()
	if d.Stream.WorkName == "" {
		d.Stream.WorkName = name
	}
	d.FileName = tsName
	d.Err = "saved as MPEG-TS (.ts): " + why.Error()
	d.mu.Unlock()
	return nil
}

// concat writes a track's pieces to w in order, repeating the init
// section whenever it changes.
func (d *Download) concat(ctx context.Context, w io.Writer, ti int, t *streamTrack) error {
	lastInit := -1
	appendFile := func(name string) error {
		f, err := os.Open(d.piecePath(ti, name))
		if os.IsNotExist(err) && t.live && !strings.HasPrefix(name, "init") {
			return nil // skipped while recording: a short jump in the file
		}
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	}
	for i := range t.segs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if in := t.segInit[i]; in >= 0 && in != lastInit {
			name := "init" + strconv.Itoa(in)
			if t.live && fileSize(d.piecePath(ti, name)) <= 0 {
				continue // a recording lost this init: its segments cannot play
			}
			if err := appendFile(name); err != nil {
				return err
			}
			lastInit = in
		}
		if err := appendFile(strconv.Itoa(i)); err != nil {
			return err
		}
	}
	return nil
}
