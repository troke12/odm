package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"odm/internal/osutil"
)

// Manager owns every download, the concurrency limit, settings and persistence.
type Manager struct {
	mu        sync.Mutex
	downloads map[string]*Download
	order     []string
	running   map[string]bool
	active    int
	client    *http.Client
	store     *Store
	settings  Settings
	setPath   string
	limiter   *Limiter
	onChange  func()
	onAction  func(Action)
	queueSeq  map[string]int
	ctx       context.Context

	// queueRun holds queues started with StartQueue. Only those run their
	// items one at a time in queue order; a download added with "start now"
	// still just competes for the global Concurrent slots.
	queueRun map[string]bool
	// drained lists started queues that ran out of work on its own (not via
	// StopQueue); whenDone consumes it so each queue's actions fire once.
	drained []string
	// pendingEnd is the strongest exit/shutdown request from finished queues,
	// held until no queue is running any more.
	pendingEnd string
	// attempts counts scheduler retries per download in a started queue.
	attempts map[string]int
	// startupDone makes "start on ODM startup" fire once even though both
	// Begin and Start run at launch.
	startupDone bool
	// limitWin is the rolling byte log for DownloadLimits; limitHit blocks
	// new transfers while the window is over the cap.
	limitWin  []limitSample
	limitLast map[string]int64
	limitHit  bool
	// schedFired remembers the day each scheduler action last ran, so a
	// one-minute HH:MM window triggers it once rather than on every tick.
	schedFired map[string]string
}

// NewManager builds a manager from persisted settings.
func NewManager(settings Settings, settingsPath string) *Manager {
	settings.normalize(settings.Dir)
	setUserAgent(settings.UserAgent)
	return &Manager{
		downloads: map[string]*Download{},
		running:   map[string]bool{},
		queueSeq:  map[string]int{},
		queueRun:  map[string]bool{},
		attempts:  map[string]int{},
		limitLast: map[string]int64{},
		client:    buildClient(settings),
		settings:  settings,
		setPath:   settingsPath,
		limiter:   NewLimiter(settings.SpeedLimit),

		schedFired: map[string]string{},
	}
}

// SystemProxy picks the proxy when ODM's options set none. It defaults to the
// environment variables; the desktop entry point replaces it with a resolver
// that also reads the OS setting, keeping this package free of OS bindings.
// Set it before NewManager.
var SystemProxy = http.ProxyFromEnvironment

// buildClient creates an HTTP client honouring the configured proxy.
func buildClient(s Settings) *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	switch s.Proxy.Mode {
	case ProxyNone:
		// Direct by choice; a nil Proxy also ignores HTTPS_PROXY.
	case ProxyHTTP, ProxySOCKS5:
		if strings.TrimSpace(s.Proxy.Host) == "" {
			transport.Proxy = SystemProxy
			break
		}
		host := net.JoinHostPort(strings.TrimSpace(s.Proxy.Host), strconv.Itoa(s.Proxy.Port))
		// net/http dials socks5 URLs itself, so one field covers both.
		pu := &url.URL{Scheme: s.Proxy.Mode, Host: host}
		if s.Proxy.User != "" {
			pu.User = url.UserPassword(s.Proxy.User, s.Proxy.Pass)
		}
		transport.Proxy = http.ProxyURL(pu)
	default:
		// ProxySystem. Leaving Proxy nil here was the old behaviour, which
		// ignored even HTTPS_PROXY, so downloads failed behind a proxy.
		transport.Proxy = SystemProxy
	}
	return &http.Client{Transport: transport}
}

// HTTPClient returns the client downloads use, so other requests (such as
// the update check) go through the same configured proxy.
func (m *Manager) HTTPClient() *http.Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.client
}

// ConfigDir is the folder holding settings.json, state and the control
// socket; empty when the manager was built without a settings path.
func (m *Manager) ConfigDir() string {
	if m.setPath == "" {
		return ""
	}
	return filepath.Dir(m.setPath)
}

// connectionsFor is the per-server exception for rawURL's host, else the
// default connection count.
func connectionsFor(st Settings, rawURL string) int {
	if u, err := url.Parse(strings.TrimSpace(rawURL)); err == nil {
		if n := st.HostConnections[strings.ToLower(u.Hostname())]; n > 0 {
			return n
		}
	}
	return st.Connections
}

// SetStore attaches persistence.
func (m *Manager) SetStore(s *Store) { m.store = s }

// SetOnChange registers a callback fired whenever any download changes.
func (m *Manager) SetOnChange(fn func()) { m.onChange = fn }

// Action is something the scheduler asks the host application to do.
type Action struct {
	Kind  string // ActionOpen, ActionExit, ActionShutdown or ActionLimit
	Queue string // queue that triggered it, if any
	Path  string // file to open, for ActionOpen
}

// Scheduler action kinds.
const (
	ActionOpen     = "open"
	ActionExit     = "exit"
	ActionShutdown = "shutdown"
	ActionLimit    = "limit" // download limit reached; downloads were paused
)

type limitSample struct {
	at    time.Time
	bytes int64
}

// SetActionHandler installs the callback for scheduler actions.
func (m *Manager) SetActionHandler(fn func(Action)) {
	m.mu.Lock()
	m.onAction = fn
	m.mu.Unlock()
}

// queueOf returns a download's queue, defaulting to the main queue.
func queueOf(d *Download) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Queue == "" {
		return DefaultQueue
	}
	return d.Queue
}

func (d *Download) order() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Order
}

// nextOrderLocked hands out the next position within a queue.
func (m *Manager) nextOrderLocked(queue string) int {
	m.queueSeq[queue]++
	return m.queueSeq[queue]
}

// MoveInQueue shifts a download up (-1) or down (+1) inside its queue and
// persists both swapped positions.
func (m *Manager) MoveInQueue(id string, delta int) bool {
	a, b := m.swapInQueue(id, delta)
	if a == nil {
		return false
	}
	// Saved outside m.mu: changed() writes the store and fires callbacks.
	m.changed(a)
	m.changed(b)
	return true
}

func (m *Manager) swapInQueue(id string, delta int) (*Download, *Download) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.downloads[id]
	if d == nil {
		return nil, nil
	}
	q := queueOf(d)
	type sibling struct {
		id    string
		order int
	}
	var sib []sibling
	for _, oid := range m.order {
		od := m.downloads[oid]
		if od == nil || queueOf(od) != q {
			continue
		}
		sib = append(sib, sibling{oid, od.order()})
	}
	sort.SliceStable(sib, func(i, j int) bool { return sib[i].order < sib[j].order })
	idx := -1
	for i := range sib {
		if sib[i].id == id {
			idx = i
			break
		}
	}
	j := idx + delta
	if idx < 0 || j < 0 || j >= len(sib) {
		return nil, nil
	}
	a, b := m.downloads[sib[idx].id], m.downloads[sib[j].id]
	oa, ob := a.order(), b.order()
	a.mu.Lock()
	a.Order = ob
	a.mu.Unlock()
	b.mu.Lock()
	b.Order = oa
	b.mu.Unlock()
	return a, b
}

// Settings returns a copy of the current settings.
func (m *Manager) Settings() Settings {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settings
}

// UpdateSettings validates, persists and applies new settings live.
func (m *Manager) UpdateSettings(next Settings) Settings {
	m.mu.Lock()
	fallback := m.settings.Dir
	proxyChanged := m.settings.Proxy != next.Proxy
	next.normalize(fallback)
	m.settings = next
	path := m.setPath
	if proxyChanged {
		m.client = buildClient(next)
	}
	m.mu.Unlock()

	m.limiter.SetRate(next.SpeedLimit)
	setUserAgent(next.UserAgent)
	if path != "" {
		_ = SaveSettings(path, next)
	}
	m.fire()
	return next
}

// credentials returns the stored Basic credentials for a host, if any.
func (m *Manager) credentials(host string) (string, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range m.settings.Logins {
		if strings.EqualFold(strings.TrimSpace(l.Host), strings.TrimSpace(host)) && l.User != "" {
			return l.User, l.Pass, true
		}
	}
	return "", "", false
}

/* ---------- queues ---------- */

// Queues returns the known queue names.
func (m *Manager) Queues() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.settings.Queues))
	copy(out, m.settings.Queues)
	return out
}

// AddQueue registers a new queue name.
func (m *Manager) AddQueue(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	m.mu.Lock()
	for _, q := range m.settings.Queues {
		if strings.EqualFold(q, name) {
			m.mu.Unlock()
			return false
		}
	}
	m.settings.Queues = append(m.settings.Queues, name)
	next := m.settings
	path := m.setPath
	m.mu.Unlock()
	if path != "" {
		_ = SaveSettings(path, next)
	}
	m.fire()
	return true
}

// MoveToQueue reassigns a download to another queue.
func (m *Manager) MoveToQueue(id, queue string) bool {
	d := m.get(id)
	if d == nil {
		return false
	}
	d.mu.Lock()
	d.Queue = queue
	d.mu.Unlock()
	m.changed(d)
	return true
}

// RemoveQueue deletes a queue and its schedule, moving its downloads to the
// main queue. The main queue cannot be removed.
func (m *Manager) RemoveQueue(name string) bool {
	if strings.EqualFold(name, DefaultQueue) {
		return false
	}
	m.mu.Lock()
	idx := -1
	for i, q := range m.settings.Queues {
		if q == name {
			idx = i
		}
	}
	if idx < 0 {
		m.mu.Unlock()
		return false
	}
	m.settings.Queues = append(append([]string(nil), m.settings.Queues[:idx]...), m.settings.Queues[idx+1:]...)
	delete(m.settings.Schedules, name)
	delete(m.queueRun, name)
	var moved []*Download
	for _, d := range m.downloads {
		if queueOf(d) == name {
			moved = append(moved, d)
		}
	}
	next, path := m.settings, m.setPath
	m.mu.Unlock()
	for _, d := range moved {
		d.mu.Lock()
		d.Queue = DefaultQueue
		d.mu.Unlock()
		m.changed(d)
	}
	if path != "" {
		_ = SaveSettings(path, next)
	}
	m.fire()
	return true
}

// StartQueue resumes every unfinished download of a queue ("*" = all queues)
// and runs each started queue sequentially, in queue order.
func (m *Manager) StartQueue(name string) {
	m.mu.Lock()
	ids := append([]string(nil), m.order...)
	m.mu.Unlock()
	var matched []*Download
	for _, id := range ids {
		d := m.get(id)
		if d == nil || d.status() == StatusComplete {
			continue
		}
		d.mu.Lock()
		match := queueMatches(d.Queue, name)
		d.mu.Unlock()
		if match {
			matched = append(matched, d)
		}
	}
	// Mark queues as running only when they actually have work; an empty
	// queue would otherwise count as instantly drained and could trigger the
	// completion action on some unrelated finish later.
	m.mu.Lock()
	for _, d := range matched {
		m.queueRun[queueOf(d)] = true
		delete(m.attempts, d.ID)
	}
	m.mu.Unlock()
	for _, d := range matched {
		m.resume(d)
	}
}

// StopQueue pauses the active downloads of a queue. A stopped queue is not
// "done", so it never triggers the completion action.
func (m *Manager) StopQueue(name string) {
	m.mu.Lock()
	for q := range m.queueRun {
		if queueMatches(q, name) {
			delete(m.queueRun, q)
		}
	}
	ids := append([]string(nil), m.order...)
	m.mu.Unlock()
	for _, id := range ids {
		d := m.get(id)
		if d == nil {
			continue
		}
		d.mu.Lock()
		match := queueMatches(d.Queue, name)
		d.mu.Unlock()
		if match {
			m.Pause(id)
		}
	}
}

func queueMatches(downloadQueue, want string) bool {
	if want == "" || want == "*" {
		return true
	}
	q := downloadQueue
	if q == "" {
		q = DefaultQueue
	}
	return strings.EqualFold(q, want)
}

/* ---------- export / import ---------- */

// Export serialises the download list as JSON.
func (m *Manager) Export() ([]byte, error) {
	m.mu.Lock()
	ids := append([]string(nil), m.order...)
	m.mu.Unlock()
	states := make([]State, 0, len(ids))
	for _, id := range ids {
		if d := m.get(id); d != nil {
			states = append(states, d.State())
		}
	}
	return json.MarshalIndent(states, "", "  ")
}

// Import merges a previously exported list, returning how many were added.
func (m *Manager) Import(data []byte) (int, error) {
	var states []State
	if err := json.Unmarshal(data, &states); err != nil {
		return 0, err
	}
	added := 0
	for _, st := range states {
		if st.URL == "" {
			continue
		}
		if st.ID == "" {
			st.ID = newID()
		}
		m.mu.Lock()
		_, exists := m.downloads[st.ID]
		stt := m.settings
		m.mu.Unlock()
		if exists {
			continue
		}
		st.Status = StatusPaused
		if st.Queue == "" {
			st.Queue = DefaultQueue
		}
		d := Restore(st, m.client)
		d.Categories = stt.Categories
		d.SetCategoryDirs(stt.CategoryDirs)
		d.SetLimiter(m.limiter)
		d.SetAuthLookup(m.credentials)
		d.SetOnUpdate(m.changed)

		m.mu.Lock()
		if d.Order == 0 {
			d.Order = m.nextOrderLocked(queueOf(d))
		}
		m.downloads[d.ID] = d
		m.order = append(m.order, d.ID)
		m.mu.Unlock()
		m.changed(d)
		added++
	}
	m.pump()
	return added, nil
}

/* ---------- lifecycle ---------- */

// Restore loads persisted downloads into the manager.
func (m *Manager) Restore(states []State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, st := range states {
		d := Restore(st, m.client)
		d.Categories = m.settings.Categories
		d.SetCategoryDirs(m.settings.CategoryDirs)
		d.SetLimiter(m.limiter)
		d.SetAuthLookup(m.credentials)
		d.SetOnUpdate(m.changed)
		if d.Order == 0 {
			d.Order = m.nextOrderLocked(queueOf(d))
		}
		m.downloads[d.ID] = d
		m.order = append(m.order, d.ID)
	}
}

// Begin binds the run context, starts any queued downloads and the queues
// set to start with ODM.
func (m *Manager) Begin(ctx context.Context) {
	m.mu.Lock()
	m.ctx = ctx
	first := !m.startupDone
	m.startupDone = true
	var onStartup []string
	if first {
		for q, sc := range m.settings.Schedules {
			if sc.OnStartup {
				onStartup = append(onStartup, q)
			}
		}
	}
	m.mu.Unlock()
	m.pump()
	for _, q := range onStartup {
		m.StartQueue(q)
	}
}

// Start begins the background sampler/pump loop; it blocks until ctx ends.
func (m *Manager) Start(ctx context.Context) {
	m.Begin(ctx)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sample()
			m.trackLimit(time.Now())
			m.checkSchedule(time.Now())
		}
	}
}

func (m *Manager) sample() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.running))
	for id := range m.running {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	any := false
	for _, id := range ids {
		if d := m.get(id); d != nil {
			d.Sample()
			any = true
		}
	}
	if any {
		m.fire()
	}
}

// AddOptions describes a new download.
type AddOptions struct {
	URL          string
	Dir          string
	Connections  int
	Headers      map[string]string
	SpeedLimit   int64
	Start        bool
	OpenWhenDone bool
	Category     string
	FileName     string
	Queue        string
	Description  string
}

// Create queues a download from opts and returns its projection.
func (m *Manager) Create(o AddOptions) Info {
	m.mu.Lock()
	st := m.settings
	m.mu.Unlock()

	dir := o.Dir
	if dir == "" {
		dir = st.Dir
	}
	conns := o.Connections
	if conns <= 0 {
		conns = connectionsFor(st, o.URL)
	}
	queue := strings.TrimSpace(o.Queue)
	if queue == "" {
		queue = DefaultQueue
	}
	d := New(newID(), o.URL, dir, conns, o.Headers, o.SpeedLimit, m.client)
	d.Categories = st.Categories
	d.CategoryName = o.Category
	d.FileName = o.FileName
	d.OpenWhenDone = o.OpenWhenDone
	d.Queue = queue
	d.Description = o.Description
	d.SetCategoryDirs(st.CategoryDirs)
	d.SetLimiter(m.limiter)
	d.SetAuthLookup(m.credentials)
	if o.SpeedLimit > 0 {
		d.SetSpeedLimit(o.SpeedLimit)
	}
	if !o.Start {
		d.mu.Lock()
		d.Status = StatusPaused
		d.mu.Unlock()
	}
	d.SetOnUpdate(m.changed)

	m.mu.Lock()
	d.Order = m.nextOrderLocked(queue)
	m.downloads[d.ID] = d
	m.order = append(m.order, d.ID)
	m.mu.Unlock()
	m.fire()
	m.pump()
	return d.Info()
}

// Probe inspects a URL without creating a download.
func (m *Manager) Probe(ctx context.Context, rawURL string, headers map[string]string) (*Probe, error) {
	return Inspect(ctx, m.client, strings.TrimSpace(rawURL), headers)
}

// SetSpeedLimit applies a per-download cap in bytes/sec.
func (m *Manager) SetSpeedLimit(id string, bytesPerSecond int64) bool {
	d := m.get(id)
	if d == nil {
		return false
	}
	d.SetSpeedLimit(bytesPerSecond)
	m.changed(d)
	return true
}

// SetOpenWhenDone toggles opening the file on completion.
func (m *Manager) SetOpenWhenDone(id string, v bool) bool {
	d := m.get(id)
	if d == nil {
		return false
	}
	d.SetOpenWhenDone(v)
	m.changed(d)
	return true
}

// FindByURL returns a download whose address is rawURL, ignoring
// surrounding spaces and a trailing fragment.
func (m *Manager) FindByURL(rawURL string) (Info, bool) {
	want := sameURLKey(rawURL)
	for _, d := range m.List() {
		if sameURLKey(d.URL) == want {
			return d, true
		}
	}
	return Info{}, false
}

func sameURLKey(raw string) string {
	raw = strings.TrimSpace(raw)
	if i := strings.IndexByte(raw, '#'); i >= 0 {
		raw = raw[:i]
	}
	return raw
}

// AddAuto adds a download requested without a person confirming it (browser
// hand-off, clipboard, grabber, API). With the DuplicateSkip policy a URL
// already in the list is not added again; the existing entry is returned
// with added=false.
func (m *Manager) AddAuto(o AddOptions) (info Info, added bool) {
	if m.Settings().DuplicateAuto != DuplicateAgain {
		if existing, ok := m.FindByURL(o.URL); ok {
			return existing, false
		}
	}
	return m.Create(o), true
}

// Readdress points an unfinished download at a new address, for links that
// expire. The new address must serve the same file: the size must match,
// and once bytes are on disk the server must allow ranges so the transfer
// continues where it stopped instead of mixing two files.
func (m *Manager) Readdress(ctx context.Context, id, newURL string) error {
	d := m.get(id)
	if d == nil {
		return fmt.Errorf("download not found")
	}
	newURL = strings.TrimSpace(newURL)
	if u, err := url.Parse(newURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q is not an http(s) address", newURL)
	}
	switch d.status() {
	case StatusDownloading, StatusQueued:
		return fmt.Errorf("pause the download before changing its address")
	case StatusComplete:
		return fmt.Errorf("the download is already complete")
	}
	d.mu.Lock()
	headers, total, done, probed := d.Headers, d.Total, d.doneLocked(), len(d.Chunks) > 0
	d.mu.Unlock()

	if !probed { // never started: the next start probes the new address
		d.mu.Lock()
		d.URL, d.FinalURL = newURL, ""
		d.mu.Unlock()
		m.changed(d)
		return nil
	}
	p, err := Inspect(ctx, m.HTTPClient(), newURL, headers)
	if err != nil {
		return fmt.Errorf("checking the new address: %w", err)
	}
	if total > 0 && p.Total > 0 && p.Total != total {
		return fmt.Errorf("the new address serves %d bytes but this download is %d bytes; it is a different file", p.Total, total)
	}
	if done > 0 && !p.RangeOK {
		return fmt.Errorf("the new address does not support resuming, so the %d bytes already downloaded cannot be kept", done)
	}
	d.mu.Lock()
	d.URL, d.FinalURL, d.cookies = newURL, p.FinalURL, p.Cookies
	d.ETag, d.LastModified = p.ETag, p.LastModified
	if d.Status == StatusError {
		d.Status, d.Err = StatusPaused, ""
	}
	d.mu.Unlock()
	m.changed(d)
	return nil
}

// Add queues a new download and returns its projection.
func (m *Manager) Add(rawURL, dir string, conns int, headers map[string]string, speedLimit int64) Info {
	return m.Create(AddOptions{URL: rawURL, Dir: dir, Connections: conns, Headers: headers, SpeedLimit: speedLimit, Start: true})
}

// List returns every download, oldest first.
func (m *Manager) List() []Info {
	m.mu.Lock()
	ids := append([]string(nil), m.order...)
	m.mu.Unlock()
	out := make([]Info, 0, len(ids))
	for _, id := range ids {
		if d := m.get(id); d != nil {
			out = append(out, d.Info())
		}
	}
	return out
}

// Get returns one download by id.
func (m *Manager) Get(id string) (Info, bool) {
	if d := m.get(id); d != nil {
		return d.Info(), true
	}
	return Info{}, false
}

// Pause stops a download.
func (m *Manager) Pause(id string) bool {
	d := m.get(id)
	if d == nil {
		return false
	}
	m.mu.Lock()
	running := m.running[id]
	m.mu.Unlock()
	if running {
		d.Pause()
	} else {
		d.mu.Lock()
		if d.Status == StatusQueued {
			d.Status = StatusPaused
		}
		d.mu.Unlock()
		m.changed(d)
	}
	return true
}

// Resume restarts a paused or failed download.
func (m *Manager) Resume(id string) bool {
	if d := m.get(id); d != nil {
		m.resume(d)
	}
	return true
}

func (m *Manager) resume(d *Download) {
	d.mu.Lock()
	if d.Status != StatusComplete {
		d.Status = StatusQueued
		d.Err = ""
		// Resume cancels a pause still waiting for Start to pick it up.
		d.pauseReq = false
	}
	d.mu.Unlock()
	m.pump()
}

// PauseAll pauses every active or queued download.
func (m *Manager) PauseAll() {
	m.mu.Lock()
	ids := append([]string(nil), m.order...)
	m.mu.Unlock()
	for _, id := range ids {
		m.Pause(id)
	}
}

// ResumeAll restarts every paused or failed download.
func (m *Manager) ResumeAll() {
	m.mu.Lock()
	ids := append([]string(nil), m.order...)
	m.mu.Unlock()
	for _, id := range ids {
		m.resume(m.get(id))
	}
}

// ClearCompleted removes finished entries (their files are kept).
func (m *Manager) ClearCompleted() int {
	m.mu.Lock()
	ids := append([]string(nil), m.order...)
	m.mu.Unlock()
	n := 0
	for _, id := range ids {
		if d := m.get(id); d != nil && d.status() == StatusComplete {
			if m.Remove(id) {
				n++
			}
		}
	}
	return n
}

// Remove stops the download and drops its state.
func (m *Manager) Remove(id string) bool { return m.RemoveWithFile(id, false) }

// RemoveWithFile stops the download, deletes its state and optionally its file.
func (m *Manager) RemoveWithFile(id string, deleteFile bool) bool {
	d := m.get(id)
	if d == nil {
		return false
	}
	d.Pause()
	d.mu.Lock()
	dir, name := d.Dir, d.FileName
	f := d.file
	d.mu.Unlock()
	if f != nil {
		_ = f.Close()
	}

	m.mu.Lock()
	delete(m.downloads, id)
	delete(m.running, id)
	for i, v := range m.order {
		if v == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.mu.Unlock()

	if name != "" {
		_ = os.Remove(filepath.Join(dir, name+".odmpart"))
		if deleteFile {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	if m.store != nil {
		_ = m.store.Delete(id)
	}
	m.fire()
	m.pump()
	return true
}

// Path returns the on-disk path of a download for open/reveal actions.
func (m *Manager) Path(id string) (string, bool) {
	d := m.get(id)
	if d == nil {
		return "", false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.FileName == "" {
		return "", false
	}
	p := filepath.Join(d.Dir, d.FileName)
	if d.Status != StatusComplete {
		p += ".odmpart"
	}
	return p, true
}

func (m *Manager) get(id string) *Download {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.downloads[id]
}

func (m *Manager) pump() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx == nil {
		return
	}
	defer m.settleQueuesLocked()
	if m.limitHit {
		return // over the download limit: nothing new starts until it clears
	}
	busy := map[string]int{}
	for id := range m.running {
		if d := m.downloads[id]; d != nil {
			busy[queueOf(d)]++
		}
	}
	for _, id := range m.orderedIDsLocked() {
		if m.active >= m.settings.Concurrent {
			return
		}
		d := m.downloads[id]
		if d == nil || m.running[id] {
			continue
		}
		if d.status() != StatusQueued {
			continue
		}
		q := queueOf(d)
		if m.queueRun[q] && busy[q] >= m.parallelLocked(q) {
			continue // a started queue runs its own number of files at once
		}
		m.running[id] = true
		m.active++
		busy[q]++
		go m.run(d)
	}
}

// settleQueuesLocked retires started queues that have nothing left to do.
// Paused items keep a queue open: the user interrupted it, it did not finish.
func (m *Manager) settleQueuesLocked() {
	if len(m.queueRun) == 0 {
		return
	}
	open := map[string]bool{}
	for id, d := range m.downloads {
		switch d.status() {
		case StatusQueued, StatusDownloading, StatusPaused:
			open[queueOf(d)] = true
		default:
			if m.running[id] {
				open[queueOf(d)] = true
			}
		}
	}
	for q := range m.queueRun {
		if !open[q] {
			delete(m.queueRun, q)
			m.drained = append(m.drained, q)
		}
	}
}

// parallelLocked is how many files a started queue downloads at once.
func (m *Manager) parallelLocked(q string) int {
	if n := m.settings.Schedules[q].Parallel; n > 0 {
		return n
	}
	return 1
}

// orderedIDsLocked lists downloads by queue, then by their queue position.
func (m *Manager) orderedIDsLocked() []string {
	ids := append([]string(nil), m.order...)
	sort.SliceStable(ids, func(i, j int) bool {
		a, b := m.downloads[ids[i]], m.downloads[ids[j]]
		if a == nil || b == nil {
			return false
		}
		qa, qb := queueOf(a), queueOf(b)
		if qa != qb {
			return qa < qb
		}
		return a.order() < b.order()
	})
	return ids
}

func (m *Manager) run(d *Download) {
	d.Start(m.ctx)
	m.mu.Lock()
	delete(m.running, d.ID)
	if m.active > 0 {
		m.active--
	}
	m.mu.Unlock()

	if d.status() == StatusComplete {
		d.mu.Lock()
		open, dir, name := d.OpenWhenDone, d.Dir, d.FileName
		d.mu.Unlock()
		if open && name != "" {
			_ = osutil.Open(filepath.Join(dir, name))
		}
	}
	if d.status() == StatusError && m.retryInQueue(d) {
		m.changed(d)
		m.pump()
		return
	}
	m.changed(d)
	m.pump()
	m.whenDone()
}

// retryInQueue re-queues a failed download when its queue is running and its
// schedule allows more attempts. Download.Start already retries connections
// with backoff, so this is for a file that failed outright.
func (m *Manager) retryInQueue(d *Download) bool {
	m.mu.Lock()
	q := queueOf(d)
	sc := m.settings.Schedules[q]
	ok := m.queueRun[q] && sc.RetriesEnabled && m.attempts[d.ID] < sc.Retries
	if ok {
		m.attempts[d.ID]++
	}
	m.mu.Unlock()
	if ok {
		d.mu.Lock()
		d.Status = StatusQueued
		d.Err = ""
		d.mu.Unlock()
	}
	return ok
}

func (m *Manager) changed(d *Download) {
	if m.store != nil {
		_ = m.store.Save(d.State())
	}
	m.fire()
}

func (m *Manager) fire() {
	if m.onChange != nil {
		m.onChange()
	}
}

func (d *Download) status() Status {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.Status
}

// checkSchedule starts and stops each queue at its configured times.
func (m *Manager) checkSchedule(now time.Time) {
	m.mu.Lock()
	scheds := make(map[string]QueueSchedule, len(m.settings.Schedules))
	for q, sc := range m.settings.Schedules {
		scheds[q] = sc
	}
	m.mu.Unlock()
	hm, day := now.Format("15:04"), now.Format("2006-01-02")
	for q, sc := range scheds {
		if sc.StartEnabled && sc.StartAt == hm && startDay(sc, now) && m.firstToday(q+"\x00start", day) {
			m.StartQueue(q)
		}
		// Stop only touches a queue the scheduler (or Start Queue) is running,
		// so downloads started by hand in that queue are left alone.
		if sc.StopEnabled && sc.StopAt == hm && m.isRunning(q) && m.firstToday(q+"\x00stop", day) {
			m.StopQueue(q)
		}
	}
}

// startDay reports whether the start time applies on now's date: the one
// date in "once" mode, otherwise the ticked weekdays.
func startDay(sc QueueSchedule, now time.Time) bool {
	if sc.OnceDate != "" {
		return now.Format("2006-01-02") == sc.OnceDate
	}
	return len(sc.Days) == 7 && sc.Days[int(now.Weekday())]
}

func (m *Manager) isRunning(q string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.queueRun[q]
}

// QueueRunning reports whether a queue was started and is still working.
func (m *Manager) QueueRunning(q string) bool { return m.isRunning(q) }

// firstToday reports whether action has not run yet on day, recording it.
func (m *Manager) firstToday(action, day string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.schedFired[action] == day {
		return false
	}
	m.schedFired[action] = day
	return true
}

// whenDone runs the finish actions of queues that completed on their own.
// "Open file" runs at once; exit and shutdown wait until no queue is running
// and nothing is transferring. An idle list alone is not enough: finishing a
// single manual download, or pausing everything, must never shut down.
func (m *Manager) whenDone() {
	m.mu.Lock()
	drained := m.drained
	m.drained = nil
	var opens []Action
	for _, q := range drained {
		sc := m.settings.Schedules[q]
		if sc.OpenWhenDone != "" {
			opens = append(opens, Action{Kind: ActionOpen, Queue: q, Path: sc.OpenWhenDone})
		}
		switch {
		case sc.ShutdownWhenDone:
			m.pendingEnd = ActionShutdown
		case sc.ExitWhenDone && m.pendingEnd == "":
			m.pendingEnd = ActionExit
		}
	}
	var end string
	if m.pendingEnd != "" && m.active == 0 && len(m.queueRun) == 0 {
		end, m.pendingEnd = m.pendingEnd, ""
	}
	handler := m.onAction
	m.mu.Unlock()
	if handler == nil {
		return
	}
	for _, a := range opens {
		handler(a)
	}
	if end != "" {
		handler(Action{Kind: end})
	}
}

// trackLimit keeps a rolling log of bytes received and, when DownloadLimits
// is on and the window total reaches the cap, pauses everything and blocks
// new transfers until enough of the window has passed.
func (m *Manager) trackLimit(now time.Time) {
	m.mu.Lock()
	lim := m.settings.Limits
	var got int64
	for id, d := range m.downloads {
		done := d.doneBytes()
		if prev, ok := m.limitLast[id]; ok && done > prev {
			got += done - prev
		}
		m.limitLast[id] = done
	}
	for id := range m.limitLast {
		if m.downloads[id] == nil {
			delete(m.limitLast, id)
		}
	}
	if got > 0 {
		m.limitWin = append(m.limitWin, limitSample{at: now, bytes: got})
	}
	cut := now.Add(-time.Duration(lim.Hours) * time.Hour)
	var total int64
	keep := m.limitWin[:0]
	for _, s := range m.limitWin {
		if s.at.After(cut) {
			keep = append(keep, s)
			total += s.bytes
		}
	}
	m.limitWin = keep
	over := lim.Enabled && total >= int64(lim.MB)<<20
	hit := over && !m.limitHit
	cleared := !over && m.limitHit
	m.limitHit = over
	handler, warn := m.onAction, !lim.NoWarning
	m.mu.Unlock()

	if hit {
		m.PauseAll()
		if handler != nil && warn {
			handler(Action{Kind: ActionLimit})
		}
	}
	if cleared {
		m.pump()
	}
}

func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return time.Now().Format("20060102150405")
	}
	return hex.EncodeToString(b)
}
