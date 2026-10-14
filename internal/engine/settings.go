package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Duplicate policies for automatic adds.
const (
	DuplicateSkip  = "skip"
	DuplicateAgain = "again"
)

// Proxy modes.
const (
	ProxySystem = "system" // OS / environment setting
	ProxyNone   = "none"   // always connect directly
	ProxyHTTP   = "http"
	ProxySOCKS5 = "socks5"
)

// ProxyConfig describes how downloads reach the network.
type ProxyConfig struct {
	Mode string `json:"mode"` // ProxySystem, ProxyNone, ProxyHTTP or ProxySOCKS5
	Host string `json:"host"`
	Port int    `json:"port"`
	User string `json:"user"`
	Pass string `json:"pass"`
}

// Login is a per-site credential applied when a download targets that host.
type Login struct {
	Host string `json:"host"`
	User string `json:"user"`
	Pass string `json:"pass"`
}

// QueueSchedule is one queue's scheduler settings.
type QueueSchedule struct {
	OnStartup    bool   `json:"on_startup,omitempty"`
	StartEnabled bool   `json:"start_enabled,omitempty"`
	StartAt      string `json:"start_at,omitempty"`  // "HH:MM"
	OnceDate     string `json:"once_date,omitempty"` // "YYYY-MM-DD"; empty = daily on Days
	Days         []bool `json:"days,omitempty"`      // Sunday..Saturday
	StopEnabled  bool   `json:"stop_enabled,omitempty"`
	StopAt       string `json:"stop_at,omitempty"` // "HH:MM"
	// Retries re-queues a file that failed while the queue was running.
	RetriesEnabled bool `json:"retries_enabled,omitempty"`
	Retries        int  `json:"retries,omitempty"`
	// What to do once the queue has run out of work on its own.
	OpenWhenDone     string `json:"open_when_done,omitempty"` // file to open
	ExitWhenDone     bool   `json:"exit_when_done,omitempty"`
	ShutdownWhenDone bool   `json:"shutdown_when_done,omitempty"`
	// Parallel is how many of the queue's files download at once.
	Parallel int `json:"parallel,omitempty"`
}

// DownloadLimits caps how much is downloaded in a rolling window.
type DownloadLimits struct {
	Enabled bool `json:"enabled"`
	MB      int  `json:"mb"`
	Hours   int  `json:"hours"`
	// NoWarning is negative so the warning defaults to on.
	NoWarning bool `json:"no_warning,omitempty"`
}

// Settings are the user-facing options persisted between runs.
type Settings struct {
	Dir          string                   `json:"dir"`
	Connections  int                      `json:"connections"`
	Concurrent   int                      `json:"concurrent"`
	SpeedLimit   int64                    `json:"speed_limit"`
	Categories   bool                     `json:"categories"`
	Clipboard    bool                     `json:"clipboard"`
	Proxy        ProxyConfig              `json:"proxy"`
	Logins       []Login                  `json:"logins"`
	Queues       []string                 `json:"queues"`
	FileTypes    []string                 `json:"file_types,omitempty"`
	CategoryDirs map[string]string        `json:"category_dirs"`
	Schedules    map[string]QueueSchedule `json:"schedules"`
	Limits       DownloadLimits           `json:"limits"`
	// HostConnections overrides Connections per server (lower-case host).
	HostConnections map[string]int `json:"host_connections,omitempty"`
	// UserAgent replaces the default User-Agent when not empty.
	UserAgent string `json:"user_agent,omitempty"`
	// DuplicateAuto decides what an automatic add (browser, clipboard,
	// grabber, API) does with a URL already in the list: DuplicateSkip or
	// DuplicateAgain. The Add dialog always asks instead.
	DuplicateAuto string `json:"duplicate_auto,omitempty"`
	// NoStartDialog skips the New Download window for downloads handed over
	// by the browser (negative so the window is shown by default).
	NoStartDialog bool `json:"no_start_dialog,omitempty"`
	// ExcludeSites are host patterns ("*.example.com", "example.org") the
	// browser extension leaves alone.
	ExcludeSites []string `json:"exclude_sites,omitempty"`
	// NoUpdateCheck is phrased negatively so settings files written before
	// the option existed keep the default: check for updates at start-up.
	NoUpdateCheck bool `json:"no_update_check,omitempty"`
	// Same negative phrasing: both windows are shown unless turned off.
	NoProgressDialog bool `json:"no_progress_dialog,omitempty"`
	NoCompleteDialog bool `json:"no_complete_dialog,omitempty"`
	// NoCloseToTray makes closing the main window quit instead of hiding it
	// to the system tray (where downloads keep running).
	NoCloseToTray bool `json:"no_close_to_tray,omitempty"`
	// FFmpegPath points at ffmpeg (the program or its folder), used to join
	// streams that come as separate video and audio. Empty: look next to
	// ODM, then on PATH.
	FFmpegPath string `json:"ffmpeg_path,omitempty"`
}

// DefaultQueue is the queue used when none is specified.
const DefaultQueue = "Main"

// DefaultSettings returns settings for a fresh install.
func DefaultSettings(dir string) Settings {
	return Settings{
		Dir:         dir,
		Connections: DefaultConnections,
		Concurrent:  DefaultFileLimit,
		Categories:  true,
		Queues:      []string{DefaultQueue},
	}
}

func (s *Settings) normalize(fallbackDir string) {
	if strings.TrimSpace(s.Dir) == "" {
		s.Dir = fallbackDir
	}
	if s.Connections <= 0 {
		s.Connections = DefaultConnections
	}
	if s.Connections > 32 {
		s.Connections = 32
	}
	if s.Concurrent <= 0 {
		s.Concurrent = DefaultFileLimit
	}
	if s.SpeedLimit < 0 {
		s.SpeedLimit = 0
	}
	switch s.Proxy.Mode {
	case ProxyNone, ProxySystem, ProxyHTTP, ProxySOCKS5:
	default:
		// Unset (fresh install) follows the OS, which is what users behind
		// a corporate proxy need without touching Options.
		s.Proxy.Mode = ProxySystem
	}
	if s.Proxy.Port == 0 {
		switch s.Proxy.Mode {
		case ProxyHTTP:
			s.Proxy.Port = 8080
		case ProxySOCKS5:
			s.Proxy.Port = 1080
		}
	}
	if len(s.FileTypes) == 0 {
		s.FileTypes = DefaultFileTypes()
	}
	if s.HostConnections == nil {
		s.HostConnections = map[string]int{}
	}
	for h, n := range s.HostConnections {
		key := strings.ToLower(strings.TrimSpace(h))
		if key == "" || n <= 0 {
			delete(s.HostConnections, h)
			continue
		}
		if n > 32 {
			n = 32
		}
		if key != h {
			delete(s.HostConnections, h)
		}
		s.HostConnections[key] = n
	}
	s.UserAgent = strings.TrimSpace(s.UserAgent)
	var sites []string
	for _, p := range s.ExcludeSites {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			sites = append(sites, p)
		}
	}
	s.ExcludeSites = sites
	if s.DuplicateAuto != DuplicateAgain {
		s.DuplicateAuto = DuplicateSkip
	}
	seen := map[string]bool{}
	queues := make([]string, 0, len(s.Queues)+1)
	for _, q := range s.Queues {
		q = strings.TrimSpace(q)
		if q == "" || seen[q] {
			continue
		}
		seen[q] = true
		queues = append(queues, q)
	}
	if !seen[DefaultQueue] {
		queues = append([]string{DefaultQueue}, queues...)
	}
	s.Queues = queues
	if s.Schedules == nil {
		s.Schedules = map[string]QueueSchedule{}
	}
	keep := map[string]bool{}
	for _, q := range s.Queues {
		keep[q] = true
		sc := s.Schedules[q]
		sc.normalize()
		s.Schedules[q] = sc
	}
	for q := range s.Schedules {
		if !keep[q] { // schedule of a deleted queue
			delete(s.Schedules, q)
		}
	}
	if s.Limits.MB <= 0 {
		s.Limits.MB = 200
	}
	if s.Limits.Hours <= 0 {
		s.Limits.Hours = 5
	}
	if s.CategoryDirs == nil {
		s.CategoryDirs = map[string]string{}
	}
}

// NewQueueSchedule returns the defaults a new queue starts with.
func NewQueueSchedule() QueueSchedule {
	var sc QueueSchedule
	sc.normalize()
	return sc
}

// normalize fills defaults and drops values the scheduler cannot use, so an
// invalid time disables that trigger instead of firing at a wrong moment.
func (sc *QueueSchedule) normalize() {
	if len(sc.Days) != 7 {
		sc.Days = []bool{true, true, true, true, true, true, true}
	}
	if !validClock(sc.StartAt) || sc.StartAt == "" {
		sc.StartAt, sc.StartEnabled = "", false
	}
	if !validClock(sc.StopAt) || sc.StopAt == "" {
		sc.StopAt, sc.StopEnabled = "", false
	}
	if sc.OnceDate != "" {
		if _, err := time.Parse("2006-01-02", sc.OnceDate); err != nil {
			sc.OnceDate = ""
		}
	}
	if sc.Parallel < 1 {
		sc.Parallel = 1
	}
	if sc.Retries <= 0 {
		sc.Retries = 10
	}
}

// validClock reports whether s is an empty string or a HH:MM time.
func validClock(s string) bool {
	if s == "" {
		return true
	}
	_, err := time.Parse("15:04", s)
	return err == nil
}

// LoadSettings reads settings from path, falling back to defaults.
func LoadSettings(path, fallbackDir string) Settings {
	s := DefaultSettings(fallbackDir)
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &s)
	}
	s.normalize(fallbackDir)
	return s
}

// SaveSettings writes settings to path.
func SaveSettings(path string, s Settings) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

var categoryExt = map[string]string{
	".mp4": "Video", ".mkv": "Video", ".avi": "Video", ".mov": "Video", ".webm": "Video",
	".flv": "Video", ".wmv": "Video", ".m4v": "Video", ".ts": "Video", ".mpg": "Video",
	".mpeg": "Video", ".3gp": "Video", ".vob": "Video", ".mts": "Video",

	".mp3": "Audio", ".m4a": "Audio", ".aac": "Audio", ".flac": "Audio", ".wav": "Audio",
	".ogg": "Audio", ".wma": "Audio", ".opus": "Audio", ".aiff": "Audio",

	".pdf": "Documents", ".doc": "Documents", ".docx": "Documents", ".xls": "Documents",
	".xlsx": "Documents", ".ppt": "Documents", ".pptx": "Documents", ".txt": "Documents",
	".epub": "Documents", ".csv": "Documents", ".rtf": "Documents", ".odt": "Documents",

	".zip": "Compressed", ".rar": "Compressed", ".7z": "Compressed", ".tar": "Compressed",
	".gz": "Compressed", ".bz2": "Compressed", ".xz": "Compressed", ".zst": "Compressed",

	".exe": "Programs", ".msi": "Programs", ".dmg": "Programs", ".pkg": "Programs",
	".deb": "Programs", ".rpm": "Programs", ".appx": "Programs", ".apk": "Programs",
	".bat": "Programs", ".sh": "Programs",

	".jpg": "Images", ".jpeg": "Images", ".png": "Images", ".gif": "Images", ".webp": "Images",
	".bmp": "Images", ".svg": "Images", ".tiff": "Images", ".ico": "Images", ".heic": "Images",

	".iso": "Disk Images", ".img": "Disk Images", ".vhd": "Disk Images",
}

// Category maps a file name to its download subfolder.
func Category(name string) string {
	if c, ok := categoryExt[strings.ToLower(filepath.Ext(name))]; ok {
		return c
	}
	return "Other"
}

// categoryOrder is the presentation order of the category folders.
var categoryOrder = []string{
	"Video", "Audio", "Documents", "Programs", "Compressed", "Images", "Disk Images", "Other",
}

// Categories returns the known category folder names, in display order.
func Categories() []string {
	out := make([]string, len(categoryOrder))
	copy(out, categoryOrder)
	return out
}

// KnownFileType reports whether name has a downloadable extension. With
// types (the Options "File types" list, with or without dots) only those
// count; without, the built-in list does. types is only compared, never
// merged into categoryExt: that map is shared read-only state and decides
// category folders, which the takeover list must not change.
func KnownFileType(name string, types ...string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	if ext == "" {
		return false
	}
	if len(types) == 0 {
		_, ok := categoryExt[ext]
		return ok
	}
	for _, t := range types {
		if ext == "."+strings.ToLower(strings.TrimPrefix(strings.TrimSpace(t), ".")) {
			return true
		}
	}
	return false
}

// DefaultFileTypes is the built-in takeover list, extensions without dots,
// sorted, as the File types tab's Default button restores it.
func DefaultFileTypes() []string {
	out := make([]string, 0, len(categoryExt))
	for ext := range categoryExt {
		out = append(out, strings.TrimPrefix(ext, "."))
	}
	sort.Strings(out)
	return out
}

// CategoryExtensions lists the extensions filed under cat, without dots.
func CategoryExtensions(cat string) []string {
	var out []string
	for ext, c := range categoryExt {
		if c == cat {
			out = append(out, strings.TrimPrefix(ext, "."))
		}
	}
	sort.Strings(out)
	return out
}
