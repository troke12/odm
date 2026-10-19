package engine

import "time"

// Status is the lifecycle state of a download.
type Status string

const (
	StatusQueued      Status = "queued"
	StatusDownloading Status = "downloading"
	StatusPaused      Status = "paused"
	StatusComplete    Status = "complete"
	StatusError       Status = "error"
)

// Tuning defaults: eight sockets per file, a minimum chunk of 64 KiB, and a
// 2 MiB step for query-range fallback.
const (
	DefaultConnections = 8
	MinChunkSize       = 64 << 10 // small enough to split mid-size files, large enough that per-request overhead stays negligible
	DefaultFileLimit   = 3
	DefaultChunkStep   = 1 << 21
	MaxRetries         = 8
	// DefaultStallTimeout is how long one connection may go without a byte,
	// including the wait for response headers, before that connection is
	// closed and its chunk is retried. The other connections of the same
	// file keep transferring.
	DefaultStallTimeout = 30 * time.Second
	// DefaultHeaderTimeout bounds the wait for a response's headers. It is
	// much longer than DefaultStallTimeout: a server that builds the file
	// before answering (an on-the-fly archive or export) sends nothing
	// until it is done.
	DefaultHeaderTimeout = 5 * time.Minute
	DefaultUserAgent     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ODM/0.1"
	// paceWindow is how long a chunk's writes are collected before its pace
	// is updated. One fast read does not decide that a connection is quick.
	paceWindow = 200 * time.Millisecond
	// paceStall is how long a response may go without its first byte
	// before its chunk counts as stalled (pace 0) for a split.
	paceStall = 5 * paceWindow
)

// Chunk is one contiguous, inclusive byte range of the target file.
type Chunk struct {
	ID    int   `json:"id"`
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Done  int64 `json:"done"`

	claimed bool
	// retired is set when the chunk leaves the plan because one connection
	// took the whole file (takeWholeFile); a worker still holding it stops.
	retired bool
	// pace is the recent bytes per second. Zero means it is not known yet,
	// so a split falls back to the largest remainder.
	pace      int64
	paceBytes int64
	paceAt    time.Time
	// fetchAt is when the current response for this chunk was asked for.
	fetchAt time.Time
}

// Size is the number of bytes the chunk covers.
func (c *Chunk) Size() int64 { return c.End - c.Start + 1 }

// Remaining is the number of bytes still to fetch for this chunk.
func (c *Chunk) Remaining() int64 { return c.Size() - c.Done }

// Complete reports whether every byte of the chunk has been written.
func (c *Chunk) Complete() bool { return c.Done >= c.Size() }

// Next is the absolute file offset the next read should write to.
func (c *Chunk) Next() int64 { return c.Start + c.Done }

// startPace begins measuring a new response: a pace from an earlier one
// (before a pause or a retry) says nothing about this connection, and its
// window would divide by the time in between.
func (c *Chunk) startPace(now time.Time) {
	c.pace, c.paceBytes, c.paceAt, c.fetchAt = 0, 0, time.Time{}, now
}

// currentPace is the chunk's pace as of now, and whether it is known. It
// decays while no bytes arrive: notePace only runs when bytes come in, so a
// hung socket would otherwise keep its last good pace forever. A response
// without its first byte for paceStall is known to be at 0.
func (c *Chunk) currentPace(now time.Time) (int64, bool) {
	if c.paceAt.IsZero() {
		if c.pace > 0 {
			return c.pace, true
		}
		if !c.fetchAt.IsZero() && now.Sub(c.fetchAt) >= paceStall {
			return 0, true
		}
		return 0, false
	}
	// Bytes since the last update over the time since: once that span is a
	// full window, it caps the pace, falling toward 0 while the socket is
	// quiet.
	if idle := now.Sub(c.paceAt); idle >= paceWindow {
		recent := int64(float64(c.paceBytes) / idle.Seconds())
		if c.pace == 0 || recent < c.pace {
			return recent, true
		}
	}
	return c.pace, c.pace > 0
}

// notePace folds n newly written bytes into the chunk's recent pace.
func (c *Chunk) notePace(n int64, now time.Time) {
	if n <= 0 {
		return
	}
	if c.paceAt.IsZero() {
		c.paceAt = now
		c.paceBytes = n
		return
	}
	c.paceBytes += n
	dt := now.Sub(c.paceAt)
	if dt < paceWindow {
		return
	}
	secs := dt.Seconds()
	if secs <= 0 {
		return
	}
	c.pace = int64(float64(c.paceBytes) / secs)
	if c.pace < 1 {
		c.pace = 1
	}
	c.paceAt = now
	c.paceBytes = 0
}

// ChunkInfo is the read-only projection handed to the UI.
type ChunkInfo struct {
	ID        int    `json:"id"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Done      int64  `json:"done"`
	Remaining int64  `json:"remaining"`
	Complete  bool   `json:"complete"`
	State     string `json:"state"`
}

// Info is the read-only projection of a download.
type Info struct {
	ID           string      `json:"id"`
	URL          string      `json:"url"`
	FinalURL     string      `json:"final_url"`
	FileName     string      `json:"file_name"`
	FilePath     string      `json:"file_path"`
	Dir          string      `json:"dir"`
	Status       Status      `json:"status"`
	Error        string      `json:"error,omitempty"`
	Total        int64       `json:"total"`
	Done         int64       `json:"done"`
	Progress     float64     `json:"progress"`
	Speed        int64       `json:"speed"`
	Connections  int         `json:"connections"`
	Active       int         `json:"active"`
	RangeOK      bool        `json:"range_ok"`
	Category     string      `json:"category"`
	Queue        string      `json:"queue"`
	Description  string      `json:"description"`
	Order        int         `json:"order"`
	SpeedLimit   int64       `json:"speed_limit"`
	OpenWhenDone bool        `json:"open_when_done"`
	Chunks       []ChunkInfo `json:"chunks"`
	// Stream is "hls" or "dash" for a segmented download, "" for a file.
	Stream string `json:"stream,omitempty"`
	// Segments and SegmentsDone count a stream's media segments across
	// all its tracks (video and separate audio), init sections left out.
	// Both are 0 until a run has read the playlist, also after a restart.
	Segments     int `json:"segments,omitempty"`
	SegmentsDone int `json:"segments_done"`
	// Live is a live stream being recorded: Pause stops and finishes it.
	// SegmentsSkipped counts its segments given up on (gaps in the file);
	// Behind means fetching is slower than the stream plays, so a lower
	// quality would keep up.
	Live            bool      `json:"live,omitempty"`
	SegmentsSkipped int       `json:"segments_skipped,omitempty"`
	Behind          bool      `json:"behind,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}
