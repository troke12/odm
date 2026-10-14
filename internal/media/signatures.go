// Package media recognises streaming manifests and media files on the web
// and parses the manifests (HLS playlists, DASH MPDs) into lists of
// segments a downloader can fetch.
//
// Everything here follows the published formats (RFC 8216 for HLS,
// ISO/IEC 23009-1 for DASH); nothing is specific to one site.
package media

import (
	"bytes"
	"math"
	"mime"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// Kind is what a response holds.
type Kind string

const (
	KindNone   Kind = ""
	KindHLS    Kind = "hls"    // M3U8 playlist
	KindDASH   Kind = "dash"   // MPD manifest
	KindSmooth Kind = "smooth" // Microsoft Smooth Streaming manifest
	KindF4M    Kind = "f4m"    // Adobe HTTP Dynamic Streaming manifest
	KindFile   Kind = "file"   // a plain audio or video file
)

// IsManifest reports whether k describes a stream that is assembled from
// segments rather than downloaded as one file.
func (k Kind) IsManifest() bool {
	return k == KindHLS || k == KindDASH || k == KindSmooth || k == KindF4M
}

// SniffLen is how much of a body Detect looks at; callers that buffer a
// response only need to keep this much.
const SniffLen = 4096

var manifestTypes = map[string]Kind{
	"application/vnd.apple.mpegurl": KindHLS,
	"application/x-mpegurl":         KindHLS,
	"audio/mpegurl":                 KindHLS,
	"audio/x-mpegurl":               KindHLS,
	"application/dash+xml":          KindDASH,
	"application/vnd.ms-sstr+xml":   KindSmooth,
	"application/f4m+xml":           KindF4M,
}

var manifestExts = map[string]Kind{
	".m3u8": KindHLS,
	".m3u":  KindHLS,
	".mpd":  KindDASH,
	".f4m":  KindF4M,
}

var fileExts = map[string]bool{
	".3gp": true, ".asf": true, ".avi": true, ".f4v": true, ".flv": true,
	".m4a": true, ".m4v": true, ".mkv": true, ".mov": true, ".mp3": true,
	".mp4": true, ".mpeg": true, ".mpg": true, ".oga": true, ".ogg": true,
	".ogv": true, ".opus": true, ".qt": true, ".wav": true, ".weba": true,
	".webm": true, ".wma": true, ".wmv": true, ".aac": true, ".flac": true,
}

// Detect classifies a response from its first bytes, its Content-Type and
// its URL, in that order of trust.
//
// The body wins because servers often mislabel playlists (text/plain,
// application/octet-stream, binary/octet-stream) and hide them behind URLs
// without an extension. Content-Type comes next. The URL extension is the
// last resort, used when the body is not available (a request seen before
// its response).
//
// Gotcha: "audio/mpegurl" is a playlist, not audio, so manifest types are
// checked before the generic audio/ and video/ prefixes. "video/mp2t" is a
// single TS segment; on its own it is reported as a file.
func Detect(contentType, rawURL string, head []byte) Kind {
	if k := sniff(head); k != KindNone {
		return k
	}
	ct, _, _ := mime.ParseMediaType(contentType)
	ct = strings.ToLower(ct)
	if k, ok := manifestTypes[ct]; ok {
		return k
	}
	if strings.HasPrefix(ct, "video/") || strings.HasPrefix(ct, "audio/") {
		return KindFile
	}
	if u, err := url.Parse(rawURL); err == nil {
		p := strings.ToLower(u.Path)
		ext := path.Ext(p)
		if k, ok := manifestExts[ext]; ok {
			return k
		}
		// Smooth Streaming URLs end in ".ism/Manifest" or ".isml/manifest".
		if strings.HasSuffix(p, "/manifest") && (strings.Contains(p, ".ism/") || strings.Contains(p, ".isml/")) {
			return KindSmooth
		}
		if fileExts[ext] {
			return KindFile
		}
	}
	return KindNone
}

// sniff recognises manifests by their opening bytes.
func sniff(head []byte) Kind {
	if len(head) > SniffLen {
		head = head[:SniffLen]
	}
	b := bytes.TrimPrefix(head, []byte("\xef\xbb\xbf")) // UTF-8 BOM
	b = bytes.TrimLeft(b, " \t\r\n")
	if bytes.HasPrefix(b, []byte("#EXTM3U")) {
		return KindHLS
	}
	if len(b) == 0 || b[0] != '<' {
		return KindNone
	}
	// XML manifests: find the root element, skipping the declaration,
	// comments and processing instructions before it.
	switch root := xmlRoot(b); root {
	case "MPD":
		return KindDASH
	case "SmoothStreamingMedia":
		return KindSmooth
	case "manifest":
		if bytes.Contains(b, []byte("ns.adobe.com/f4m")) {
			return KindF4M
		}
	}
	return KindNone
}

// xmlRoot returns the local name of the first element in b, or "".
func xmlRoot(b []byte) string {
	for len(b) > 0 {
		i := bytes.IndexByte(b, '<')
		if i < 0 {
			return ""
		}
		b = b[i+1:]
		switch {
		case bytes.HasPrefix(b, []byte("?")):
			if j := bytes.Index(b, []byte("?>")); j >= 0 {
				b = b[j+2:]
				continue
			}
			return ""
		case bytes.HasPrefix(b, []byte("!--")):
			if j := bytes.Index(b, []byte("-->")); j >= 0 {
				b = b[j+3:]
				continue
			}
			return ""
		case bytes.HasPrefix(b, []byte("!")): // DOCTYPE
			if j := bytes.IndexByte(b, '>'); j >= 0 {
				b = b[j+1:]
				continue
			}
			return ""
		}
		end := bytes.IndexAny(b, " \t\r\n/>")
		if end < 0 {
			return ""
		}
		name := string(b[:end])
		if k := strings.LastIndexByte(name, ':'); k >= 0 { // namespace prefix
			name = name[k+1:]
		}
		return name
	}
	return ""
}

// ByteRange is part of a resource: Length bytes starting at Offset.
type ByteRange struct {
	Offset, Length int64
}

// Header returns the value for an HTTP Range header.
func (r ByteRange) Header() string {
	return "bytes=" + strconv.FormatInt(r.Offset, 10) + "-" + strconv.FormatInt(r.Offset+r.Length-1, 10)
}

// seconds converts a parsed number of seconds to a Duration, refusing
// NaN, infinities and values a Duration cannot hold (about 292 years),
// which strconv.ParseFloat happily accepts from hostile input.
func seconds(v float64) (time.Duration, bool) {
	// >=: MaxInt64/1e9 multiplied back rounds up to 2^63, one past the end.
	if math.IsNaN(v) || v < 0 || v >= float64(math.MaxInt64)/float64(time.Second) {
		return 0, false
	}
	return time.Duration(v * float64(time.Second)), true
}

// resolve makes ref absolute against base; a nil base leaves ref as is.
func resolve(base *url.URL, ref string) string {
	ref = strings.TrimSpace(ref)
	if base == nil {
		return ref
	}
	u, err := base.Parse(ref)
	if err != nil {
		return ref
	}
	return u.String()
}
