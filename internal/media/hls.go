package media

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// HLS playlists come in two shapes (RFC 8216 §4.3): a master playlist that
// lists variants (one per quality) and alternative renditions (separate
// audio tracks, subtitles), and a media playlist that lists the segments
// of one variant. ParseHLS returns whichever it was given; a downloader
// picks a variant, fetches its media playlist and parses that next.

// HLSPlaylist is a parsed playlist; exactly one of Master and Media is set.
type HLSPlaylist struct {
	Master *HLSMaster
	Media  *HLSMedia
}

// HLSMaster lists the variants of a stream.
type HLSMaster struct {
	Variants   []HLSVariant
	Renditions []HLSRendition
}

// HLSVariant is one quality (EXT-X-STREAM-INF).
type HLSVariant struct {
	URL              string
	Bandwidth        int64 // peak bits per second
	AverageBandwidth int64
	Width, Height    int
	FrameRate        float64
	Codecs           string
	Audio            string // group ID of the audio renditions to pair with
	Video            string
	Subtitles        string
}

// HLSRendition is an alternative track (EXT-X-MEDIA). URL is empty when
// the rendition is already muxed into the variants.
type HLSRendition struct {
	Type       string // AUDIO, VIDEO, SUBTITLES, CLOSED-CAPTIONS
	GroupID    string
	Name       string
	Language   string
	URL        string
	Default    bool
	Autoselect bool
}

// HLSMedia is the segment list of one variant or rendition.
type HLSMedia struct {
	TargetDuration time.Duration
	MediaSequence  int64
	PlaylistType   string // "VOD", "EVENT" or ""
	Ended          bool   // EXT-X-ENDLIST seen: nothing more will be added
	Segments       []HLSSegment
	// Protected is set when segments are encrypted with a method or key
	// system other than plain AES-128 (SAMPLE-AES, FairPlay, Widevine,
	// PlayReady…). Those cannot be downloaded into a playable file.
	Protected bool
}

// Live reports whether the playlist may still grow and has to be reloaded.
func (m *HLSMedia) Live() bool { return !m.Ended && m.PlaylistType != "VOD" }

// Duration is the sum of the segment durations.
func (m *HLSMedia) Duration() time.Duration {
	var d time.Duration
	for _, s := range m.Segments {
		d += s.Duration
	}
	return d
}

// HLSSegment is one piece of the stream.
type HLSSegment struct {
	URL           string
	Sequence      int64
	Duration      time.Duration
	Range         *ByteRange // nil: the whole resource
	Key           *HLSKey    // nil: not encrypted
	Map           *HLSMap    // initialisation section (fMP4), nil for TS
	Discontinuity bool       // encoding parameters change before this one
}

// HLSKey is an EXT-X-KEY with METHOD=AES-128.
type HLSKey struct {
	Method string // "AES-128"
	URL    string // where to fetch the 16-byte key
	IV     []byte // 16 bytes, or nil to derive it from the sequence number
}

// IVFor returns the IV for the segment with the given media sequence
// number. Without an explicit IV the spec uses the sequence number as a
// 128-bit big-endian integer (RFC 8216 §5.2).
func (k *HLSKey) IVFor(sequence int64) []byte {
	if len(k.IV) == 16 {
		return k.IV
	}
	iv := make([]byte, 16)
	binary.BigEndian.PutUint64(iv[8:], uint64(sequence))
	return iv
}

// HLSMap is an EXT-X-MAP: the initialisation section prepended once.
type HLSMap struct {
	URL   string
	Range *ByteRange
	// Key is the AES-128 key in effect where the tag appears; it applies
	// to the section too (RFC 8216 §4.3.2.4), and then needs an explicit
	// IV (§4.3.2.5).
	Key *HLSKey
}

// ErrNotHLS is returned for input that does not start with #EXTM3U.
var ErrNotHLS = errors.New("media: not an HLS playlist")

// ParseHLS parses a playlist; base resolves the relative URIs in it.
//
// Gotchas handled:
//   - Attribute values may be quoted strings containing commas.
//   - EXT-X-BYTERANGE without an offset continues where the previous
//     sub-range of the same resource ended.
//   - EXT-X-KEY and EXT-X-MAP stay in effect for every following segment
//     until replaced; METHOD=NONE ends encryption.
//   - Several EXT-X-KEY tags can apply at once, one per KEYFORMAT; only
//     the "identity" one is a key ODM can use. A segment protected only by
//     other key systems marks the playlist Protected.
//   - Unknown tags are ignored, as the spec requires.
func ParseHLS(base *url.URL, data []byte) (*HLSPlaylist, error) {
	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))
	// Blank lines before the header are tolerated, as Detect does.
	data = bytes.TrimLeft(data, " \t\r\n")
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)

	first := true
	var (
		master            HLSMaster
		media             HLSMedia
		isMaster, isMedia bool

		pendingVariant *HLSVariant
		next           HLSSegment // tags collected for the next URI
		haveInf        bool
		nextRange      *ByteRange
		rangeOpen      bool // next range has no explicit offset
		key            *HLSKey
		otherKey       bool   // a non-identity key is in effect
		otherMethod    string // its METHOD
		curMap         *HLSMap
		lastRangeEnd   = map[string]int64{}
		seq            int64
		seqSet         bool
	)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if first {
			if !strings.HasPrefix(line, "#EXTM3U") {
				return nil, ErrNotHLS
			}
			first = false
			continue
		}
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "#") { // a URI line
			uri := resolve(base, line)
			switch {
			case pendingVariant != nil:
				pendingVariant.URL = uri
				master.Variants = append(master.Variants, *pendingVariant)
				pendingVariant = nil
			case haveInf:
				if !seqSet {
					seq, seqSet = media.MediaSequence, true
				}
				s := next
				s.URL = uri
				s.Sequence = seq
				s.Key = key
				s.Map = curMap
				if key == nil && otherKey {
					media.Protected = true
				}
				if nextRange != nil {
					r := *nextRange
					if rangeOpen {
						r.Offset = lastRangeEnd[uri]
					}
					if r.Offset > math.MaxInt64-r.Length {
						return nil, fmt.Errorf("media: byte range past the end of %s", line)
					}
					lastRangeEnd[uri] = r.Offset + r.Length
					s.Range = &r
				}
				media.Segments = append(media.Segments, s)
				seq++
				next, haveInf, nextRange, rangeOpen = HLSSegment{}, false, nil, false
			}
			continue
		}

		tag, value, _ := strings.Cut(line, ":")
		switch tag {
		case "#EXT-X-STREAM-INF":
			isMaster = true
			a := parseAttrs(value)
			v := HLSVariant{
				Bandwidth:        atoi64(a["BANDWIDTH"]),
				AverageBandwidth: atoi64(a["AVERAGE-BANDWIDTH"]),
				Codecs:           a["CODECS"],
				Audio:            a["AUDIO"],
				Video:            a["VIDEO"],
				Subtitles:        a["SUBTITLES"],
			}
			v.Width, v.Height = parseResolution(a["RESOLUTION"])
			v.FrameRate, _ = strconv.ParseFloat(a["FRAME-RATE"], 64)
			pendingVariant = &v
		case "#EXT-X-MEDIA":
			isMaster = true
			a := parseAttrs(value)
			r := HLSRendition{
				Type: a["TYPE"], GroupID: a["GROUP-ID"], Name: a["NAME"], Language: a["LANGUAGE"],
				Default: a["DEFAULT"] == "YES", Autoselect: a["AUTOSELECT"] == "YES",
			}
			if u, ok := a["URI"]; ok {
				r.URL = resolve(base, u)
			}
			master.Renditions = append(master.Renditions, r)
		case "#EXTINF":
			isMedia = true
			durStr, _, _ := strings.Cut(value, ",")
			sec, err := strconv.ParseFloat(strings.TrimSpace(durStr), 64)
			if sec < 0 { // "-1": unknown length, common in plain .m3u lists
				sec = 0
			}
			d, ok := seconds(sec)
			if err != nil || !ok {
				return nil, fmt.Errorf("media: bad EXTINF %q", value)
			}
			next.Duration = d
			haveInf = true
		case "#EXT-X-BYTERANGE":
			r, open, err := parseByteRange(value)
			if err != nil {
				return nil, err
			}
			nextRange, rangeOpen = &r, open
		case "#EXT-X-DISCONTINUITY":
			next.Discontinuity = true
		case "#EXT-X-TARGETDURATION":
			isMedia = true
			media.TargetDuration = time.Duration(atoi64(value)) * time.Second
		case "#EXT-X-MEDIA-SEQUENCE":
			media.MediaSequence = atoi64(value)
		case "#EXT-X-PLAYLIST-TYPE":
			media.PlaylistType = strings.TrimSpace(value)
		case "#EXT-X-ENDLIST":
			media.Ended = true
		case "#EXT-X-KEY":
			a := parseAttrs(value)
			format := a["KEYFORMAT"]
			method := a["METHOD"]
			if method == "NONE" {
				key, otherKey = nil, false
				break
			}
			// Keys with different KEYFORMATs may apply to one segment
			// only if they produce the same decryption key (RFC 8216
			// §4.3.2.4), which a different METHOD cannot; so such a key
			// replaces the other kind instead of joining it. Erring
			// towards Protected beats decrypting with the wrong key.
			if format != "" && format != "identity" {
				if key != nil && key.Method != method {
					key = nil
				}
				otherKey, otherMethod = true, method
				break
			}
			if method != "AES-128" { // SAMPLE-AES, SAMPLE-AES-CTR
				key, otherKey, otherMethod = nil, true, method
				break
			}
			if otherKey && otherMethod != method {
				otherKey = false
			}
			if a["URI"] == "" {
				return nil, errors.New("media: AES-128 key without a URI")
			}
			k := &HLSKey{Method: method, URL: resolve(base, a["URI"])}
			if iv := a["IV"]; iv != "" {
				b, err := hex.DecodeString(strings.TrimPrefix(strings.TrimPrefix(iv, "0x"), "0X"))
				if err != nil || len(b) != 16 {
					return nil, fmt.Errorf("media: bad key IV %q", iv)
				}
				k.IV = b
			}
			key = k
		case "#EXT-X-MAP":
			a := parseAttrs(value)
			m := &HLSMap{URL: resolve(base, a["URI"]), Key: key}
			if br := a["BYTERANGE"]; br != "" {
				r, _, err := parseByteRange(br) // offset defaults to 0 here
				if err != nil {
					return nil, err
				}
				m.Range = &r
			}
			curMap = m
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("media: reading playlist: %w", err)
	}
	if first {
		return nil, ErrNotHLS
	}
	switch {
	case isMaster:
		if len(master.Variants) == 0 && len(master.Renditions) == 0 {
			return nil, errors.New("media: master playlist lists no streams")
		}
		return &HLSPlaylist{Master: &master}, nil
	case isMedia:
		return &HLSPlaylist{Media: &media}, nil
	}
	return nil, errors.New("media: playlist has no variants or segments")
}

// parseAttrs reads an attribute list: NAME=value pairs separated by commas,
// where quoted values may themselves contain commas.
func parseAttrs(s string) map[string]string {
	out := map[string]string{}
	for len(s) > 0 {
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			break
		}
		name := strings.TrimSpace(s[:eq])
		s = s[eq+1:]
		var val string
		if strings.HasPrefix(s, `"`) {
			end := strings.IndexByte(s[1:], '"')
			if end < 0 {
				val, s = s[1:], ""
			} else {
				val, s = s[1:end+1], s[end+2:]
			}
			if i := strings.IndexByte(s, ','); i >= 0 {
				s = s[i+1:]
			} else {
				s = ""
			}
		} else if i := strings.IndexByte(s, ','); i >= 0 {
			val, s = s[:i], s[i+1:]
		} else {
			val, s = s, ""
		}
		out[name] = strings.TrimSpace(val)
	}
	return out
}

// parseByteRange reads "<length>[@<offset>]"; open is true when the
// offset was omitted.
func parseByteRange(s string) (r ByteRange, open bool, err error) {
	n, o, hasOffset := strings.Cut(strings.TrimSpace(s), "@")
	r.Length, err = strconv.ParseInt(n, 10, 64)
	if err == nil && hasOffset {
		r.Offset, err = strconv.ParseInt(o, 10, 64)
	}
	// The last check keeps Offset+Length from overflowing (EXT-X-MAP too).
	if err != nil || r.Length <= 0 || r.Offset < 0 || r.Offset > math.MaxInt64-r.Length {
		return ByteRange{}, false, fmt.Errorf("media: bad byte range %q", s)
	}
	return r, !hasOffset, nil
}

func parseResolution(s string) (w, h int) {
	ws, hs, ok := strings.Cut(strings.ToLower(s), "x")
	if !ok {
		return 0, 0
	}
	w, _ = strconv.Atoi(ws)
	h, _ = strconv.Atoi(hs)
	return w, h
}

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n
}
