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
	DefaultUserAgent    = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ODM/0.1"
)

// Chunk is one contiguous, inclusive byte range of the target file.
type Chunk struct {
	ID    int   `json:"id"`
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Done  int64 `json:"done"`

	claimed bool
}

// Size is the number of bytes the chunk covers.
func (c *Chunk) Size() int64 { return c.End - c.Start + 1 }

// Remaining is the number of bytes still to fetch for this chunk.
func (c *Chunk) Remaining() int64 { return c.Size() - c.Done }

// Complete reports whether every byte of the chunk has been written.
func (c *Chunk) Complete() bool { return c.Done >= c.Size() }

// Next is the absolute file offset the next read should write to.
func (c *Chunk) Next() int64 { return c.Start + c.Done }

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
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
}
