package engine

import (
	"context"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

// Probe captures what a target advertises before the transfer starts.
type Probe struct {
	FinalURL     string
	Total        int64
	RangeOK      bool
	FileName     string
	ContentType  string
	ETag         string
	LastModified string
	Cookies      []*http.Cookie
}

// Inspect issues a ranged GET for the first byte to learn the size, whether
// the server honours Range requests, the suggested file name and cookies.
func Inspect(ctx context.Context, client *http.Client, rawURL string, headers map[string]string) (*Probe, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	// One byte. A range that covers the whole file may be answered with 200,
	// which would hide that the server supports ranges.
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("Accept-Encoding", "identity")
	applyHeaders(req, headers)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	p := &Probe{
		FinalURL:     resp.Request.URL.String(),
		ContentType:  resp.Header.Get("Content-Type"),
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
		Cookies:      resp.Cookies(),
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		if _, _, total, ok := ParseContentRange(resp.Header.Get("Content-Range")); ok {
			p.RangeOK = true
			p.Total = total
		}
	case http.StatusOK:
		p.Total = resp.ContentLength
	default:
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}
	if p.Total <= 0 {
		return nil, fmt.Errorf("server did not report a content length")
	}
	p.FileName = FileName(resp, rawURL)
	return p, nil
}

// ParseContentRange parses "bytes 1-10/100" or "bytes */100".
func ParseContentRange(v string) (start, end, total int64, ok bool) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(strings.ToLower(v), "bytes") {
		return 0, 0, 0, false
	}
	v = strings.TrimSpace(v[len("bytes"):])
	slash := strings.IndexByte(v, '/')
	if slash < 0 {
		return 0, 0, 0, false
	}
	rangePart := strings.TrimSpace(v[:slash])
	totalPart := strings.TrimSpace(v[slash+1:])
	if rangePart != "*" {
		dash := strings.IndexByte(rangePart, '-')
		if dash < 0 {
			return 0, 0, 0, false
		}
		s, err1 := strconv.ParseInt(strings.TrimSpace(rangePart[:dash]), 10, 64)
		e, err2 := strconv.ParseInt(strings.TrimSpace(rangePart[dash+1:]), 10, 64)
		if err1 != nil || err2 != nil {
			return 0, 0, 0, false
		}
		start, end = s, e
	}
	if totalPart != "*" {
		t, err := strconv.ParseInt(totalPart, 10, 64)
		if err != nil {
			return 0, 0, 0, false
		}
		total = t
	}
	return start, end, total, true
}

// FileName derives a safe local file name from the response and URL.
func FileName(resp *http.Response, rawURL string) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if fn := params["filename"]; fn != "" {
				if s := SanitizeName(fn); s != "" {
					return s
				}
			}
		}
	}
	if u, err := url.Parse(resp.Request.URL.String()); err == nil {
		if base := path.Base(u.Path); base != "" && base != "/" && base != "." {
			if s := SanitizeName(base); s != "" {
				return s
			}
		}
	}
	if u, err := url.Parse(rawURL); err == nil {
		if base := path.Base(u.Path); base != "" && base != "/" && base != "." {
			if s := SanitizeName(base); s != "" {
				return s
			}
		}
	}
	return "download.bin"
}

// SanitizeName strips path separators and characters illegal on Windows.
func SanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, ":", "_")
	for _, r := range []string{"<", ">", "\"", "|", "?", "*"} {
		name = strings.ReplaceAll(name, r, "_")
	}
	name = strings.Trim(name, ". ")
	if name == "" || name == "." || name == ".." {
		return ""
	}
	if len(name) > 200 {
		name = name[:200]
	}
	return name
}
