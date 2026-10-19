package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Download is a single managed download job.
type Download struct {
	mu sync.Mutex

	ID           string
	URL          string
	FinalURL     string
	FileName     string
	Dir          string
	BaseDir      string
	Categories   bool
	CategoryName string
	Headers      map[string]string
	Connections  int
	SpeedLimit   int64
	OpenWhenDone bool
	Queue        string
	Description  string
	Order        int

	Total   int64
	RangeOK bool
	Status  Status
	Err     string
	Chunks  []*Chunk
	// Stream is set for a segmented (HLS/DASH) download; see stream.go.
	Stream *StreamJob
	// TrustFrom, when set, replaces URL as the address localGuard judges
	// (AddOptions.TrustFrom).
	TrustFrom string

	CreatedAt time.Time
	UpdatedAt time.Time

	client *http.Client
	// h1 serves several ranges at once over HTTP/1.1. The shared client may
	// use HTTP/2, which would put every range on one socket. It is guarded
	// like guarded, and rebuilt on the same changes (h1From, h1For).
	h1      *http.Client
	h1From  *http.Client
	h1For   string
	limiter *Limiter
	shared  *Limiter
	cancel  context.CancelCauseFunc
	// abortCtx/abort belong to the current run; removed is set for good
	// once the download is being removed (see abandon).
	abortCtx context.Context
	abort    context.CancelFunc
	removed  bool
	// pauseReq records a Pause that arrived while no transfer context exists
	// yet (the manager has dispatched the download but Start has not run), so
	// Start honours it instead of the pause being silently dropped.
	pauseReq bool
	// retryBase is the first pause between attempts. Tests set a short
	// value. Zero keeps the 300ms default.
	retryBase time.Duration
	file      *os.File
	cookies   []*http.Cookie
	auth      func(host string) (string, string, bool)
	catDirs   map[string]string
	onUpdate  func(*Download)

	lastDone int64
	lastAt   time.Time
	speed    int64
	notifyAt time.Time

	// runDone is closed when the current Start returns (nil before any).
	runDone chan struct{}

	// Segmented downloads: bytes on disk, and segments present / total.
	streamDone             int64
	streamHave, streamSegs int
	// Live recordings: segments given up on, and whether fetching falls
	// behind the stream (live.go checkPace).
	streamSkipped int
	liveBehind    bool
	// guarded is client wrapped by guardLocal for stream requests. It is
	// rebuilt when the client or the manifest (Readdress) changes, since
	// its verdict on the manifest is cached.
	guarded, guardedFrom *http.Client
	guardedFor           string

	// stallTimeout and headerTimeout override DefaultStallTimeout and
	// DefaultHeaderTimeout when set. Tests use short values; a zero value
	// means the default.
	stallTimeout, headerTimeout time.Duration
	// ETag and LastModified identify the representation on the server.
	// A resume sends one of them as If-Range and starts over when it no
	// longer matches.
	ETag         string
	LastModified string

	discardPart bool
	changed     bool
	// transferCancel stops the connections of the current pass. It is
	// separate from cancel so a changed file or an answered range can end
	// the pass without pausing.
	transferCancel context.CancelFunc
	// alone is set once a ranged request is answered with the whole file.
	// Later passes and a resumed download use one connection.
	alone bool
	// sawIgnored is set for the pass that noticed that answer, so Start
	// fetches the remainder after the other connections have stopped.
	sawIgnored bool
	// plan changes when that answer is noticed. A read still holding the
	// previous plan does not write those bytes into the replacement chunk.
	plan int
}

// New creates a queued download. Probing happens on the first Start.
func New(id, rawURL, dir string, conns int, headers map[string]string, speedLimit int64, client *http.Client) *Download {
	if conns <= 0 {
		conns = DefaultConnections
	}
	return &Download{
		ID:          id,
		URL:         rawURL,
		Dir:         dir,
		BaseDir:     dir,
		Connections: conns,
		Headers:     headers,
		SpeedLimit:  speedLimit,
		Status:      StatusQueued,
		client:      client,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
}

// SetLimiter attaches a shared rate limiter (a nil limiter disables limiting).
func (d *Download) SetLimiter(l *Limiter) {
	d.mu.Lock()
	d.shared = l
	if d.limiter == nil {
		d.limiter = l
	}
	d.mu.Unlock()
}

// SetSpeedLimit sets a per-download cap in bytes/sec (0 restores the shared one).
func (d *Download) SetSpeedLimit(bytesPerSecond int64) {
	d.mu.Lock()
	if bytesPerSecond > 0 {
		d.limiter = NewLimiter(bytesPerSecond)
	} else {
		d.limiter = d.shared
	}
	d.SpeedLimit = bytesPerSecond
	d.mu.Unlock()
}

// SetOpenWhenDone toggles opening the file once the transfer completes.
func (d *Download) SetOpenWhenDone(v bool) {
	d.mu.Lock()
	d.OpenWhenDone = v
	d.mu.Unlock()
}

// SetAuthLookup installs the per-host credential lookup used for Basic auth.
func (d *Download) SetAuthLookup(fn func(host string) (string, string, bool)) {
	d.mu.Lock()
	d.auth = fn
	d.mu.Unlock()
}

// SetCategoryDirs installs per-category destination folders.
func (d *Download) SetCategoryDirs(dirs map[string]string) {
	cp := make(map[string]string, len(dirs))
	for k, v := range dirs {
		cp[k] = v
	}
	d.mu.Lock()
	d.catDirs = cp
	d.mu.Unlock()
}

// SetOnUpdate registers a callback invoked when visible state changes.
func (d *Download) SetOnUpdate(fn func(*Download)) { d.onUpdate = fn }

// Start runs the transfer to completion, pause or error. It blocks.
func (d *Download) Start(parent context.Context) {
	ctx, cancel := context.WithCancelCause(parent)
	// abortCtx outlives a Stop: a recording assembles under it after its
	// own ctx is done, and only removing the download cancels it.
	abortCtx, abort := context.WithCancel(context.Background())
	defer abort()
	done := make(chan struct{})
	d.mu.Lock()
	d.runDone = done
	d.abortCtx, d.abort = abortCtx, abort
	if d.removed {
		abort()
	}
	d.mu.Unlock()
	defer close(done)
	defer func() {
		cancel(nil)
		d.mu.Lock()
		// Drop the spent cancel func: a Pause between this run and the next
		// must set pauseReq, not cancel a context that is already gone.
		d.cancel = nil
		d.pauseReq = false
		// A paused or failed run keeps no handle on the .odmpart: the next
		// run opens it again, and Windows refuses to rename or delete a
		// file that is still open.
		f := d.file
		d.file = nil
		h1 := d.h1
		d.mu.Unlock()
		if f != nil {
			_ = f.Close()
		}
		// The HTTP/1.1 client's pool belongs to this download alone: its
		// idle sockets would otherwise stay open after the run.
		if h1 != nil {
			h1.CloseIdleConnections()
		}
	}()
	d.mu.Lock()
	if d.pauseReq {
		d.Status = StatusPaused
		d.mu.Unlock()
		d.notify(true)
		return
	}
	d.cancel = cancel
	d.Status = StatusDownloading
	d.Err = ""
	d.lastDone = 0
	d.lastAt = time.Now()
	d.mu.Unlock()
	d.notify(true)

	if d.Stream != nil {
		d.runStream(ctx)
		d.notify(true)
		return
	}

	// Passes over the same download. One extra pass covers a file that
	// changes after the probe: the pass stops, the partial file is dropped,
	// and the next pass fetches the new file; a further change is reported
	// as an error. A 200 for a smaller range keeps the stored prefix and
	// runs once more on one connection, without probing again.
	changes := 0
	needPrepare := true
	for {
		if needPrepare {
			if err := d.prepare(ctx); err != nil {
				// A cancelled context means pause (or shutdown) during the probe, not
				// a failure; same rule as the ctx.Err() check after the workers.
				if ctx.Err() != nil {
					d.mu.Lock()
					d.Status = StatusPaused
					d.mu.Unlock()
				} else {
					d.fail(err)
				}
				d.notify(true)
				return
			}
			needPrepare = false
		}

		transferCtx, transferCancel := context.WithCancel(ctx)
		d.mu.Lock()
		d.transferCancel = transferCancel
		d.sawIgnored = false
		workers := d.Connections
		if !d.RangeOK || d.alone {
			workers = 1
		}
		d.mu.Unlock()
		if workers < 1 {
			workers = 1
		}
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); d.worker(transferCtx) }()
		}
		wg.Wait()
		transferCancel()
		d.mu.Lock()
		d.transferCancel = nil
		paused := ctx.Err() != nil
		complete := d.allCompleteLocked()
		status := d.Status
		changed := d.changed
		d.changed = false
		again := d.sawIgnored
		d.mu.Unlock()

		if paused {
			if again {
				d.planAlone()
			}
			d.mu.Lock()
			d.Status = StatusPaused
			d.mu.Unlock()
			d.notify(true)
			return
		}
		if changed {
			if changes == 1 {
				d.fail(errRepresentationChanged)
				d.notify(true)
				return
			}
			changes++
			needPrepare = true
			d.mu.Lock()
			d.Status = StatusDownloading
			d.Err = ""
			d.mu.Unlock()
			continue
		}
		if again && status != StatusError {
			d.planAlone()
			d.mu.Lock()
			complete = d.allCompleteLocked()
			d.mu.Unlock()
			if !complete {
				continue
			}
		}

		switch {
		case complete:
			if err := d.finish(); err != nil {
				d.fail(err)
			} else {
				d.mu.Lock()
				d.Status = StatusComplete
				d.mu.Unlock()
			}
		case status != StatusError:
			d.fail(errors.New("transfer ended with unfinished chunks"))
		}
		d.notify(true)
		return
	}
}

func (d *Download) prepare(ctx context.Context) error {
	d.mu.Lock()
	needProbe := len(d.Chunks) == 0
	rawURL, headers := d.URL, d.Headers
	d.mu.Unlock()

	if needProbe {
		p, err := Inspect(ctx, d.guardedClient(), rawURL, headers)
		if err != nil {
			return err
		}
		d.applyProbe(p)
	} else if err := d.revalidate(ctx); err != nil {
		return err
	}
	return d.openTarget(needProbe)
}

// applyProbe stores a first probe and builds the chunk plan. The caller
// does not hold d.mu.
func (d *Download) applyProbe(p *Probe) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.FinalURL = p.FinalURL
	if d.FileName == "" {
		d.FileName = p.FileName
	}
	d.Total = p.Total
	d.RangeOK = p.RangeOK
	d.ETag = p.ETag
	d.LastModified = p.LastModified
	d.cookies = p.Cookies
	if d.Connections <= 0 {
		d.Connections = DefaultConnections
	}
	if d.SpeedLimit != 0 && d.limiter == nil {
		d.limiter = NewLimiter(d.SpeedLimit)
	}
	d.planLocked(p)
}

// revalidate runs before a resume. When the saved ETag, Last-Modified or
// size no longer matches, the partial file is discarded and planned again.
//
// It asks the address the transfer itself uses (FinalURL, with the cookies
// the first probe got), as a resume did before this check existed: a
// short-lived redirect or token link may no longer work from d.URL. A probe
// that fails is not fatal for the same reason; the resume goes on as it
// used to, and If-Range on every ranged request still catches a changed
// file. A refusal by the local-network guard is final, as everywhere.
func (d *Download) revalidate(ctx context.Context) error {
	d.mu.Lock()
	rawURL := d.FinalURL
	if rawURL == "" {
		rawURL = d.URL
	}
	headers := make(map[string]string, len(d.Headers)+1)
	for k, v := range d.Headers {
		headers[k] = v
	}
	if len(d.cookies) > 0 {
		headers["Cookie"] = cookieHeader(d.cookies)
	}
	etag, modified, total, ranged := d.ETag, d.LastModified, d.Total, d.RangeOK
	d.mu.Unlock()

	p, err := Inspect(ctx, d.guardedClient(), rawURL, headers)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, errLocalNetwork) {
			return err
		}
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// A probe that sets no cookie leaves the saved ones in place.
	if len(p.Cookies) > 0 {
		d.cookies = p.Cookies
	}
	if p.FinalURL != "" {
		d.FinalURL = p.FinalURL
	}
	if !representationDiffers(etag, modified, total, p) {
		if p.ETag != "" {
			d.ETag = p.ETag
		}
		if p.LastModified != "" {
			d.LastModified = p.LastModified
		}
		// The same file from a server that now answers ranges with the
		// whole file: the bytes on disk are still good, so the download
		// keeps its prefix and continues on one connection, as when a
		// transfer notices it (noteIgnoredRange).
		if ranged && !p.RangeOK && !d.alone {
			d.alone = true
			d.plan++
			d.planAloneLocked()
		}
		return nil
	}
	d.ETag = p.ETag
	d.LastModified = p.LastModified
	d.Total = p.Total
	d.RangeOK = p.RangeOK
	d.planLocked(p)
	d.discardPart = true
	return nil
}

// planLocked replaces the chunk list from a probe. The caller holds d.mu.
func (d *Download) planLocked(p *Probe) {
	conns := d.Connections
	if conns < 1 {
		conns = 1
	}
	if p.RangeOK {
		d.Chunks = NewPlan(p.Total, conns)
		return
	}
	d.Chunks = []*Chunk{{ID: 0, Start: 0, End: p.Total - 1}}
}

// representationDiffers reports whether p is a different file from the one
// already partly stored. A missing saved validator does not count, so state
// written before validators were kept still resumes. A different size does.
// A server that no longer accepts ranges does not by itself: that is the
// same file, continued on one connection (see revalidate).
func representationDiffers(etag, modified string, total int64, p *Probe) bool {
	if total > 0 && p.Total > 0 && total != p.Total {
		return true
	}
	if etag != "" && p.ETag != "" && etag != p.ETag {
		return true
	}
	if modified != "" && p.LastModified != "" && modified != p.LastModified {
		return true
	}
	return false
}

// openTarget opens the .odmpart file. A fresh download (first probe) claims
// a name nobody uses yet, because finishing used to delete whatever file
// already had the name, and two downloads with one name shared an .odmpart.
// A resumed download keeps the name it claimed.
func (d *Download) openTarget(fresh bool) error {
	d.mu.Lock()
	if d.FileName == "" {
		d.FileName = "download.bin"
	}
	discard := d.discardPart
	d.discardPart = false
	existing := d.file
	dir := d.BaseDir
	if dir == "" {
		dir = d.Dir
	}
	if d.Categories {
		cat := d.CategoryName
		if cat == "" {
			cat = Category(d.FileName)
		}
		if custom := d.catDirs[cat]; strings.TrimSpace(custom) != "" {
			dir = custom
		} else {
			dir = filepath.Join(dir, cat)
		}
	}
	d.Dir = dir
	final := filepath.Join(dir, d.FileName)
	tmp := final + ".odmpart"
	// A stream assembles its file at the end; pre-sizing it would only
	// double the disk space used while the segments are on disk too.
	rangeOK, total := d.RangeOK && d.Stream == nil, d.Total
	d.mu.Unlock()

	if existing != nil && !fresh {
		if discard {
			if err := existing.Truncate(0); err != nil {
				return err
			}
		}
		if rangeOK && total > 0 {
			return existing.Truncate(total)
		}
		return nil
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var f *os.File
	var err error
	if fresh {
		var name string
		name, f, err = claimName(dir, filepath.Base(final))
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.FileName = name
		d.mu.Unlock()
	} else {
		f, err = os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
	}
	if discard {
		if err := f.Truncate(0); err != nil {
			_ = f.Close()
			return err
		}
	}
	if rangeOK && total > 0 {
		if err := f.Truncate(total); err != nil {
			_ = f.Close()
			return err
		}
	}
	d.mu.Lock()
	d.file = f
	d.mu.Unlock()
	return nil
}

func (d *Download) worker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		ch := d.takeChunk()
		if ch == nil {
			ch = d.trySplit()
		}
		if ch == nil {
			return
		}
		err := d.fetchWithRetry(ctx, ch)
		if errors.Is(err, errRepresentationChanged) {
			d.noteChanged()
			d.mu.Lock()
			ch.claimed = false
			d.mu.Unlock()
			return
		}
		d.mu.Lock()
		ch.claimed = false
		ignored := errors.Is(err, errIgnoredRange)
		// A cancelled pass, including a pause, is not a failed chunk.
		if err != nil && !ignored && ctx.Err() == nil {
			d.Err = err.Error()
			d.Status = StatusError
		}
		d.mu.Unlock()
		if ignored {
			d.noteIgnoredRange()
			return
		}
		if err != nil {
			return
		}
	}
}

func (d *Download) takeChunk() *Chunk {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.Chunks {
		if !c.Complete() && !c.claimed {
			c.claimed = true
			return c
		}
	}
	return nil
}

func (d *Download) trySplit() *Chunk {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.alone {
		return nil
	}
	nc, ok := splitAt(d.Chunks, MinChunkSize, time.Now())
	if !ok {
		return nil
	}
	nc.claimed = true
	d.Chunks = append(d.Chunks, nc)
	return nc
}

func (d *Download) fetchWithRetry(ctx context.Context, ch *Chunk) error {
	var err error
	backoff := d.retryDelay()
	for attempt := 0; attempt < MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = d.fetch(ctx, ch)
		if err == nil || errors.Is(err, errIgnoredRange) {
			return err
		}
		if errors.Is(err, errRepresentationChanged) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errLocalNetwork) {
			return err // the same refusal on every try
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

func (d *Download) fetch(ctx context.Context, ch *Chunk) error {
	d.mu.Lock()
	if ch.Complete() {
		d.mu.Unlock()
		return nil
	}
	start, end := ch.Next(), ch.End
	ch.startPace(time.Now())
	rangeOK := d.RangeOK
	conns := d.Connections
	if d.alone {
		conns = 1 // one connection after an ignored range: no need for HTTP/1.1
	}
	finalURL := d.FinalURL
	headers := d.Headers
	cookies := d.cookies
	etag, modified := d.ETag, d.LastModified
	total := d.Total
	alone := d.alone
	plan := d.plan
	d.mu.Unlock()
	validator, isETag := ifRangeValidator(etag, modified)
	client := d.segmentClient(rangeOK, conns)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, finalURL, nil)
	if err != nil {
		return err
	}
	applyHeaders(req, headers)
	if rangeOK {
		// The body is written at this offset, so it must be the file's own
		// bytes. Go's transport already leaves out gzip when a Range is
		// set; this is for servers that compress unasked, and it replaces
		// an Accept-Encoding set on the download, which would ask for the
		// very compressed bytes that cannot be written here.
		req.Header.Set("Accept-Encoding", "identity")
		if validator != "" {
			req.Header.Set("If-Range", validator)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}
	if d.auth != nil {
		if u, perr := url.Parse(finalURL); perr == nil {
			if user, pass, ok := d.auth(u.Hostname()); ok {
				req.SetBasicAuth(user, pass)
			}
		}
	}
	if len(cookies) > 0 {
		req.Header.Set("Cookie", cookieHeader(cookies))
	}

	resp, err := d.exchange(ctx, client, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Created before the status checks: skipping a stored prefix reads the
	// body too, and a connection that stalls there is retried like any other.
	idle := newIdleCloser(resp.Body, d.readStall())
	defer idle.stop()

	if rangeOK {
		switch resp.StatusCode {
		case http.StatusPartialContent:
			if cs, _, _, ok := ParseContentRange(resp.Header.Get("Content-Range")); ok && cs != start {
				return fmt.Errorf("range mismatch: asked %d, got %d", start, cs)
			}
		case http.StatusOK:
			// A 200 to a request with If-Range is either a changed file (the
			// validator no longer matches, so the server sends the new one
			// whole) or a server that ignores Range. The 200's own validator
			// tells them apart: a different one restarts the download. The
			// same one, or a 200 when no If-Range was sent, is the whole
			// file this download is fetching.
			if validator != "" && !sameValidator(validator, isETag, resp) {
				return errRepresentationChanged
			}
			// A shorter length is not that file.
			if resp.ContentLength >= 0 && total > 0 && resp.ContentLength != total {
				return fmt.Errorf("server ignored Range (status %s)", resp.Status)
			}
			if !wholeFileRange(start, end, total) {
				if !alone {
					return errIgnoredRange
				}
				// This connection is already the only one. Skip the prefix
				// that is already stored and write the rest at its offset.
				if err := discardPrefix(ctx, idle, resp.Body, start); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("server ignored Range (status %s)", resp.Status)
		}
	} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
	buf := make([]byte, 64*1024)
	pos := start
	early := false
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, rerr := idle.read(resp.Body, buf)
		if rerr != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if n > 0 {
			d.mu.Lock()
			if d.plan != plan {
				d.mu.Unlock()
				return errIgnoredRange
			}
			limit := ch.End
			d.mu.Unlock()
			if limit >= 0 && pos+int64(n) > limit+1 {
				n = int(limit + 1 - pos)
			}
			if n > 0 {
				if _, werr := d.file.WriteAt(buf[:n], pos); werr != nil {
					return werr
				}
				pos += int64(n)
				d.mu.Lock()
				ch.Done += int64(n)
				ch.notePace(int64(n), time.Now())
				d.mu.Unlock()
				d.notify(false)
				if werr := d.limiter.Wait(ctx, n); werr != nil {
					return werr
				}
			}
			if limit >= 0 && pos > limit {
				break
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				early = true
				break
			}
			return rerr
		}
	}
	// A body that ended short after some bytes is not a failure: the
	// worker takes the chunk up again from its last byte, so a server that
	// caps each 206 (at 1 MiB, say) still finishes a large chunk. Only a
	// response that wrote nothing counts against MaxRetries, with the
	// usual backoff, rather than being asked again at once forever.
	if early && pos == start {
		d.mu.Lock()
		complete := ch.Complete()
		d.mu.Unlock()
		if !complete {
			return errShortBody
		}
	}
	return nil
}

// errStalled means a connection produced no bytes within the idle limit
// while reading the body, or no response headers within the header limit.
// The chunk is retried from the last byte written. A pause is not a stall:
// the caller returns the context error before counting a retry.
var errStalled = errors.New("connection stalled")

// exchange sends req and returns when the response headers arrive. The
// header limit bounds that wait, then it is stopped so a long body is not
// cut short. It is longer than the stall limit: some servers build the file
// (an archive, an export) before they answer at all. Pausing cancels ctx
// and is reported as ctx.Err().
func (d *Download) exchange(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	headerCtx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(d.headerWait(), cancel)
	resp, err := client.Do(req.WithContext(headerCtx))
	if timer.Stop() {
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
		return resp, nil
	}
	cancel()
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, errStalled
}

// cancelOnClose releases the header context once the body is finished.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

func (d *Download) readStall() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stallTimeout > 0 {
		return d.stallTimeout
	}
	return DefaultStallTimeout
}

func (d *Download) headerWait() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.headerTimeout > 0 {
		return d.headerTimeout
	}
	return DefaultHeaderTimeout
}

// idleCloser closes a body that goes idle too long within one Read. One
// timer per connection, armed only around Read: a goroutine and timer per
// 64 KiB read would sit on the hottest path, and time spent in the speed
// limiter between reads is not the server's stall.
type idleCloser struct {
	idle    time.Duration
	timer   *time.Timer
	stalled atomic.Bool
}

func newIdleCloser(body io.Closer, idle time.Duration) *idleCloser {
	c := &idleCloser{idle: idle}
	c.timer = time.AfterFunc(idle, func() {
		c.stalled.Store(true)
		_ = body.Close()
	})
	c.timer.Stop()
	return c
}

// read reads once from body, or returns errStalled when the timer closed
// the body because no bytes came in time. A Read cut short that way ends
// in an error from the closed body, reported as the stall instead.
func (c *idleCloser) read(body io.Reader, buf []byte) (int, error) {
	c.timer.Reset(c.idle)
	n, err := body.Read(buf)
	if !c.timer.Stop() && c.stalled.Load() {
		return 0, errStalled
	}
	return n, err
}

func (c *idleCloser) stop() { c.timer.Stop() }

// errShortBody means a response ended cleanly (io.EOF) without writing any
// byte of the chunk. A connection cut mid-body with a Content-Length gives
// io.ErrUnexpectedEOF instead, which was already retried.
var errShortBody = errors.New("connection closed early")

func (d *Download) retryDelay() time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.retryBase > 0 {
		return d.retryBase
	}
	return 300 * time.Millisecond
}

// errRepresentationChanged means the server no longer serves the bytes this
// download has partly stored. The partial file is discarded and fetched again.
var errRepresentationChanged = errors.New("file on the server changed")

// errIgnoredRange means the server answered a smaller range with the whole
// file. The body is not written at the range offset. Finished bytes stay,
// and the download continues on one connection.
var errIgnoredRange = errors.New("server returned the whole file for a range")

// wholeFileRange reports whether [start, end] already covers the whole file.
// end < 0 is an open-ended range.
func wholeFileRange(start, end, total int64) bool {
	if start != 0 {
		return false
	}
	if total <= 0 {
		return end < 0
	}
	return end < 0 || end >= total-1
}

// discardPrefix reads n bytes and drops them. A 200 body starts at byte 0,
// so the single connection skips the prefix already stored. Reads go
// through idle, so a stall here is retried as in the transfer itself.
func discardPrefix(ctx context.Context, idle *idleCloser, body io.Reader, n int64) error {
	buf := make([]byte, 64*1024)
	var dropped int64
	for dropped < n {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := n - dropped
		if want > int64(len(buf)) {
			want = int64(len(buf))
		}
		nr, err := idle.read(body, buf[:want])
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		dropped += int64(nr)
		if err != nil {
			if errors.Is(err, io.EOF) && dropped >= n {
				return nil
			}
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("whole-file response ended before byte %d", n)
			}
			return err
		}
	}
	return nil
}

// noteIgnoredRange records that a range was answered with the whole file
// and stops the other connections of this pass. The plan is rebuilt after
// they have stopped.
func (d *Download) noteIgnoredRange() {
	d.mu.Lock()
	if d.alone {
		d.mu.Unlock()
		return
	}
	d.alone = true
	d.sawIgnored = true
	d.plan++
	cancel := d.transferCancel
	d.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// planAlone keeps the contiguous prefix and leaves one chunk for the rest.
// Every connection has stopped, so chunk progress is stable.
func (d *Download) planAlone() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.planAloneLocked()
}

// planAloneLocked is planAlone for a caller that holds d.mu.
func (d *Download) planAloneLocked() {
	if d.Total <= 0 {
		return
	}
	gap := contiguousDone(d.Chunks, d.Total)
	if gap > d.Total {
		gap = d.Total
	}
	d.Chunks = []*Chunk{{ID: 0, Start: 0, End: d.Total - 1, Done: gap}}
}

// contiguousDone is the first offset, starting at 0, that no chunk has stored.
func contiguousDone(chunks []*Chunk, total int64) int64 {
	off := int64(0)
	for {
		if total > 0 && off >= total {
			return total
		}
		next := off
		for _, c := range chunks {
			if c == nil || c.Start > off {
				continue
			}
			have := c.Start + c.Done
			if c.End >= 0 && have > c.End+1 {
				have = c.End + 1
			}
			if have > next {
				next = have
			}
		}
		if next == off {
			return off
		}
		off = next
	}
}

// ifRangeValidator is what a ranged request sends as If-Range, and whether
// it is an ETag: the ETag when it is strong, else Last-Modified, else
// nothing. A weak ETag ("W/") is not allowed there (RFC 9110, 13.1.5), and
// servers such as Go's own http.ServeContent answer it with a full 200:
// every ranged request of a fresh download would look like a changed file.
func ifRangeValidator(etag, modified string) (string, bool) {
	if etag != "" && !strings.HasPrefix(etag, "W/") {
		return etag, true
	}
	return modified, false
}

// sameValidator reports whether resp carries the validator that was sent
// as If-Range. A 200 without it cannot show the file is the same, so it
// counts as changed.
func sameValidator(validator string, isETag bool, resp *http.Response) bool {
	if isETag {
		return resp.Header.Get("ETag") == validator
	}
	return resp.Header.Get("Last-Modified") == validator
}

func (d *Download) noteChanged() {
	d.mu.Lock()
	d.changed = true
	cancel := d.transferCancel
	d.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// segmentClient returns the HTTP/1.1 client when more than one ranged
// connection will run. One stream keeps the shared client. Both go through
// the local-network guard, like every request of a download.
func (d *Download) segmentClient(rangeOK bool, conns int) *http.Client {
	if !rangeOK || conns <= 1 {
		return d.guardedClient()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	manifest := d.guardManifestLocked()
	if d.h1 != nil && d.h1From == d.client && d.h1For == manifest {
		return d.h1
	}
	src := d.client
	if src == nil {
		src = http.DefaultClient
	}
	var base *http.Transport
	if src.Transport != nil {
		tr, ok := src.Transport.(*http.Transport)
		if !ok {
			// Not a transport this can copy: the shared client, guarded.
			if d.guarded == nil || d.guardedFrom != d.client || d.guardedFor != manifest {
				d.guarded, d.guardedFrom, d.guardedFor = guardLocal(d.client, manifest), d.client, manifest
			}
			return d.guarded
		}
		base = tr
	}
	if d.h1 != nil {
		d.h1.CloseIdleConnections() // its pool is its own
	}
	h1 := &http.Client{
		Transport:     segmentTransport(base),
		CheckRedirect: src.CheckRedirect,
		Jar:           src.Jar,
		Timeout:       src.Timeout,
	}
	d.h1, d.h1From, d.h1For = guardLocal(h1, manifest), d.client, manifest
	return d.h1
}

func (d *Download) finish() error {
	d.mu.Lock()
	f := d.file
	dir, name := d.Dir, d.FileName
	d.file = nil
	d.mu.Unlock()

	if f != nil {
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	final := filepath.Join(dir, name)
	tmp := final + ".odmpart"
	if _, err := os.Stat(final); err == nil {
		// A file took this name while we downloaded. Keep it and finish
		// under the next free name; this used to delete it.
		alt, err := freeName(dir, name)
		if err != nil {
			return err
		}
		d.mu.Lock()
		d.FileName = alt
		d.mu.Unlock()
		final = filepath.Join(dir, alt)
	}
	return os.Rename(tmp, final)
}

// maxNameTries bounds the "name (n).ext" search.
const maxNameTries = 1000

// claimName finds the first free name starting from name and creates its
// .odmpart with O_EXCL, so a concurrent download cannot take the same one.
func claimName(dir, name string) (string, *os.File, error) {
	for i := 1; i <= maxNameTries; i++ {
		cand := numberedName(name, i)
		final := filepath.Join(dir, cand)
		if _, err := os.Stat(final); err == nil {
			continue
		}
		f, err := os.OpenFile(final+".odmpart", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		if os.IsExist(err) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		return cand, f, nil
	}
	return "", nil, fmt.Errorf("no free file name for %q in %s", name, dir)
}

// freeName is the first numbered variant of name with neither a file nor
// an .odmpart in dir.
func freeName(dir, name string) (string, error) {
	for i := 2; i <= maxNameTries; i++ {
		cand := numberedName(name, i)
		final := filepath.Join(dir, cand)
		_, e1 := os.Stat(final)
		_, e2 := os.Stat(final + ".odmpart")
		if os.IsNotExist(e1) && os.IsNotExist(e2) {
			return cand, nil
		}
	}
	return "", fmt.Errorf("no free file name for %q in %s", name, dir)
}

// numberedName returns name for n == 1, else "base (n).ext", keeping
// compound archive extensions whole ("x (2).tar.gz", not "x.tar (2).gz").
func numberedName(name string, n int) string {
	if n <= 1 {
		return name
	}
	ext := filepath.Ext(name)
	lower := strings.ToLower(name)
	for _, c := range []string{".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst"} {
		if strings.HasSuffix(lower, c) {
			ext = name[len(name)-len(c):]
			break
		}
	}
	return fmt.Sprintf("%s (%d)%s", strings.TrimSuffix(name, ext), n, ext)
}

// Pause stops the running transfer, preserving progress.
func (d *Download) Pause() { d.stop(nil) }

// abandon stops the download for removal. A live recording, which a Pause
// finishes into a file, then assembles nothing (see recordLive).
//
// It also cancels the run's abortCtx, under which a recording that was
// already stopped may still be assembling: a context cause cannot do that,
// as the first cause (the Stop) is the one kept.
func (d *Download) abandon() {
	d.mu.Lock()
	d.removed = true
	abort := d.abort
	d.mu.Unlock()
	if abort != nil {
		abort()
	}
	d.stop(errRemoved)
}

// isRemoved reports whether abandon was called.
func (d *Download) isRemoved() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.removed
}

// assemblyContext is the context a recording assembles under after it was
// stopped: cancelled only by removal.
func (d *Download) assemblyContext() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.abortCtx == nil {
		return context.Background()
	}
	return d.abortCtx
}

func (d *Download) stop(cause error) {
	d.mu.Lock()
	cancel := d.cancel
	if cancel == nil {
		d.pauseReq = true
	}
	d.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
}

// waitIdle waits up to timeout for a running Start to return, so its
// files are closed before someone deletes them (Windows refuses to delete
// open files).
func (d *Download) waitIdle(timeout time.Duration) {
	d.mu.Lock()
	done := d.runDone
	d.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// Sample recomputes the transfer speed; call roughly once per second.
func (d *Download) Sample() {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	done := d.doneLocked()
	if !d.lastAt.IsZero() {
		dt := now.Sub(d.lastAt).Seconds()
		if dt > 0 {
			d.speed = int64(float64(done-d.lastDone) / dt)
			if d.speed < 0 { // a stream drops a failed segment's bytes
				d.speed = 0
			}
		}
	}
	d.lastDone, d.lastAt = done, now
}

// State serialises the durable parts of a download.
type State struct {
	ID           string            `json:"id"`
	URL          string            `json:"url"`
	FinalURL     string            `json:"final_url"`
	FileName     string            `json:"file_name"`
	Dir          string            `json:"dir"`
	BaseDir      string            `json:"base_dir,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Connections  int               `json:"connections"`
	SpeedLimit   int64             `json:"speed_limit,omitempty"`
	OpenWhenDone bool              `json:"open_when_done,omitempty"`
	Total        int64             `json:"total"`
	RangeOK      bool              `json:"range_ok"`
	ETag         string            `json:"etag,omitempty"`
	LastModified string            `json:"last_modified,omitempty"`
	Alone        bool              `json:"alone,omitempty"`
	CategoryName string            `json:"category_name,omitempty"`
	Queue        string            `json:"queue,omitempty"`
	Description  string            `json:"description,omitempty"`
	Order        int               `json:"order,omitempty"`
	Status       Status            `json:"status"`
	Err          string            `json:"error,omitempty"`
	Chunks       []*Chunk          `json:"chunks"`
	Stream       *StreamJob        `json:"stream,omitempty"`
	TrustFrom    string            `json:"trust_from,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// State snapshots the download for persistence.
func (d *Download) State() State {
	d.mu.Lock()
	defer d.mu.Unlock()
	chunks := make([]*Chunk, len(d.Chunks))
	for i, c := range d.Chunks {
		cc := *c
		cc.claimed = false
		chunks[i] = &cc
	}
	var stream *StreamJob
	if d.Stream != nil {
		sj := *d.Stream
		sj.Counts = append([]int(nil), d.Stream.Counts...)
		sj.Done = d.streamDone
		stream = &sj
	}
	status := d.Status
	if status == StatusDownloading {
		status = StatusPaused
	}
	return State{
		ID: d.ID, URL: d.URL, FinalURL: d.FinalURL, FileName: d.FileName, Dir: d.Dir,
		BaseDir: d.BaseDir, Headers: d.Headers, Connections: d.Connections, SpeedLimit: d.SpeedLimit,
		OpenWhenDone: d.OpenWhenDone, CategoryName: d.CategoryName,
		Queue: d.Queue, Description: d.Description, Order: d.Order,
		Total: d.Total, RangeOK: d.RangeOK, ETag: d.ETag, LastModified: d.LastModified, Alone: d.alone,
		Status: status, Err: d.Err,
		Chunks: chunks, Stream: stream, TrustFrom: d.TrustFrom, CreatedAt: d.CreatedAt, UpdatedAt: time.Now(),
	}
}

// Restore rebuilds a download from persisted state.
func Restore(s State, client *http.Client) *Download {
	if s.Connections <= 0 {
		s.Connections = DefaultConnections
	}
	d := New(s.ID, s.URL, s.Dir, s.Connections, s.Headers, s.SpeedLimit, client)
	d.mu.Lock()
	d.FinalURL = s.FinalURL
	d.FileName = s.FileName
	if s.BaseDir != "" {
		d.BaseDir = s.BaseDir
	}
	d.Dir = s.Dir
	d.Total = s.Total
	d.RangeOK = s.RangeOK
	d.ETag = s.ETag
	d.LastModified = s.LastModified
	d.alone = s.Alone
	d.OpenWhenDone = s.OpenWhenDone
	d.CategoryName = s.CategoryName
	d.Queue = s.Queue
	d.Description = s.Description
	d.Order = s.Order
	d.Chunks = s.Chunks
	d.Stream = s.Stream
	d.TrustFrom = s.TrustFrom
	if s.Stream != nil {
		d.streamDone = s.Stream.Done
		if s.Status == StatusComplete {
			d.streamDone = s.Total
		}
	}
	d.Err = s.Err
	d.CreatedAt = s.CreatedAt
	switch s.Status {
	case StatusComplete:
		d.Status = StatusComplete
	case StatusError:
		d.Status = StatusError
	default:
		d.Status = StatusPaused
	}
	for _, c := range d.Chunks {
		c.claimed = false
	}
	d.mu.Unlock()
	return d
}

// Info projects the download for the API/UI.
func (d *Download) Info() Info {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.infoLocked()
}

func (d *Download) category() string {
	if d.CategoryName != "" {
		return d.CategoryName
	}
	return Category(d.FileName)
}

func (d *Download) infoLocked() Info {
	done := d.doneLocked()
	var pct float64
	if d.Total > 0 {
		pct = float64(done) / float64(d.Total) * 100
	}
	chunks := make([]ChunkInfo, 0, len(d.Chunks))
	active := 0
	for _, c := range d.Chunks {
		state := "Waiting"
		switch {
		case c.Complete():
			state = "Complete"
		case c.claimed:
			state = "Receiving…"
			active++
		}
		chunks = append(chunks, ChunkInfo{
			ID: c.ID, Start: c.Start, End: c.End, Done: c.Done,
			Remaining: c.Remaining(), Complete: c.Complete(), State: state,
		})
	}
	path := ""
	if d.FileName != "" {
		path = filepath.Join(d.Dir, d.FileName)
		if d.Status != StatusComplete {
			path += ".odmpart"
		}
	}
	stream := ""
	if d.Stream != nil {
		stream = string(d.Stream.Kind)
	}
	return Info{
		ID: d.ID, URL: d.URL, FinalURL: d.FinalURL, FileName: d.FileName, FilePath: path,
		Dir: d.Dir, Status: d.Status, Error: d.Err, Total: d.Total, Done: done,
		Progress: pct, Speed: d.speed, Connections: d.Connections, Active: active,
		RangeOK: d.RangeOK, Category: d.category(), Queue: d.Queue, Description: d.Description, Order: d.Order, Chunks: chunks,
		SpeedLimit: d.SpeedLimit, OpenWhenDone: d.OpenWhenDone,
		Stream: stream, Segments: d.streamSegs, SegmentsDone: d.streamHave,
		Live:            d.Stream != nil && d.Stream.Live,
		SegmentsSkipped: d.streamSkipped, Behind: d.liveBehind,
		CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
	}
}

// doneBytes is the number of bytes received so far.
func (d *Download) doneBytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.doneLocked()
}

func (d *Download) doneLocked() int64 {
	done := d.streamDone
	for _, c := range d.Chunks {
		done += c.Done
	}
	return done
}

func (d *Download) allCompleteLocked() bool {
	if len(d.Chunks) == 0 {
		return false
	}
	for _, c := range d.Chunks {
		if !c.Complete() {
			return false
		}
	}
	return true
}

func (d *Download) fail(err error) {
	d.mu.Lock()
	d.Status = StatusError
	d.Err = err.Error()
	d.mu.Unlock()
}

func (d *Download) notify(force bool) {
	d.mu.Lock()
	d.UpdatedAt = time.Now()
	d.mu.Unlock()
	if d.onUpdate == nil {
		return
	}
	d.mu.Lock()
	if !force && time.Since(d.notifyAt) < 250*time.Millisecond {
		d.mu.Unlock()
		return
	}
	d.notifyAt = time.Now()
	d.mu.Unlock()
	d.onUpdate(d)
}

func cookieHeader(cookies []*http.Cookie) string {
	parts := make([]string, 0, len(cookies))
	for _, c := range cookies {
		parts = append(parts, c.Name+"="+c.Value)
	}
	return strings.Join(parts, "; ")
}
