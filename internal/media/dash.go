package media

import (
	"encoding/xml"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// A DASH manifest (MPD) describes Periods; each Period has AdaptationSets
// (one per kind of track: video, audio per language, subtitles), and each
// AdaptationSet has Representations (one per quality). ParseDASH expands
// every Representation into the concrete list of segment URLs, so a
// downloader picks one video and one audio Representation and fetches
// their Init + Segments in order.

// DASHManifest is a parsed MPD.
type DASHManifest struct {
	// Live is set for type="dynamic" manifests. Their segment lists depend
	// on the wall clock: a SegmentTimeline is expanded as far as it is
	// written (a trailing r="-1" counts once), a number-based template is
	// not expanded at all. Either way the manifest has to be reloaded.
	Live     bool
	Duration time.Duration
	Periods  []DASHPeriod
}

// DASHPeriod is one consecutive part of the presentation (multi-period
// manifests often splice ads or chapters).
type DASHPeriod struct {
	ID              string
	Start, Duration time.Duration
	Representations []DASHRepresentation
}

// DASHRepresentation is one encoded version of one track.
type DASHRepresentation struct {
	ID          string
	MimeType    string // "video/mp4", "audio/webm"…
	ContentType string // "video", "audio", "text" when the MPD says so
	Codecs      string
	Lang        string
	Bandwidth   int64
	Width       int
	Height      int
	FrameRate   string // as written: "30", "30000/1001"
	// Protected is set when the MPD declares ContentProtection (DRM or
	// common encryption): the segments download but will not play.
	Protected bool
	Init      *DASHSegment // initialisation segment, nil when there is none
	Segments  []DASHSegment
}

// IsVideo and IsAudio classify the track.
func (r DASHRepresentation) IsVideo() bool {
	return r.ContentType == "video" || strings.HasPrefix(r.MimeType, "video/")
}
func (r DASHRepresentation) IsAudio() bool {
	return r.ContentType == "audio" || strings.HasPrefix(r.MimeType, "audio/")
}

// DASHSegment is one resource to fetch, or a byte range of it.
type DASHSegment struct {
	URL      string
	Range    *ByteRange
	Duration time.Duration // 0 when unknown (SegmentBase, SegmentList without durations)
}

// Expansion limits. A template set once at Period level is inherited by
// every Representation, so a small MPD with a tiny segment duration, a
// long presentation and many empty <Representation/> elements would
// otherwise allocate without limit; one limit per Representation is not
// enough. The global limit is what a hostile manifest can make ODM
// allocate (about 100 bytes per segment, so ~100 MB). Real manifests stay
// below both: 24 h in 2 s segments is 43,200 per track, and ten tracks of
// that is 432,000.
const (
	maxSegmentsPerRep = 200000
	maxSegments       = 1000000 // whole manifest
)

type mpdXML struct {
	Type     string      `xml:"type,attr"`
	Duration string      `xml:"mediaPresentationDuration,attr"`
	BaseURLs []string    `xml:"BaseURL"`
	Periods  []periodXML `xml:"Period"`
}

type segmentInfo struct {
	Template *segTemplateXML `xml:"SegmentTemplate"`
	List     *segListXML     `xml:"SegmentList"`
}

type periodXML struct {
	ID       string     `xml:"id,attr"`
	Start    string     `xml:"start,attr"`
	Duration string     `xml:"duration,attr"`
	BaseURLs []string   `xml:"BaseURL"`
	Sets     []adaptXML `xml:"AdaptationSet"`
	segmentInfo
}

type protectionXML struct {
	Scheme string `xml:"schemeIdUri,attr"`
}

// trackAttrs are the attributes a Representation inherits from its
// AdaptationSet when it does not set them itself.
type trackAttrs struct {
	MimeType    string          `xml:"mimeType,attr"`
	ContentType string          `xml:"contentType,attr"`
	Codecs      string          `xml:"codecs,attr"`
	Width       string          `xml:"width,attr"`
	Height      string          `xml:"height,attr"`
	FrameRate   string          `xml:"frameRate,attr"`
	Protection  []protectionXML `xml:"ContentProtection"`
}

type adaptXML struct {
	Lang     string    `xml:"lang,attr"`
	BaseURLs []string  `xml:"BaseURL"`
	Reps     []reprXML `xml:"Representation"`
	trackAttrs
	segmentInfo
}

type reprXML struct {
	ID        string   `xml:"id,attr"`
	Bandwidth string   `xml:"bandwidth,attr"`
	BaseURLs  []string `xml:"BaseURL"`
	trackAttrs
	segmentInfo
}

type segTemplateXML struct {
	Media          string       `xml:"media,attr"`
	Initialization string       `xml:"initialization,attr"`
	StartNumber    string       `xml:"startNumber,attr"`
	Timescale      string       `xml:"timescale,attr"`
	Duration       string       `xml:"duration,attr"`
	PTO            string       `xml:"presentationTimeOffset,attr"`
	Timeline       *timelineXML `xml:"SegmentTimeline"`
}

type timelineXML struct {
	S []struct {
		T string `xml:"t,attr"`
		D string `xml:"d,attr"`
		R string `xml:"r,attr"`
	} `xml:"S"`
}

type initXML struct {
	SourceURL string `xml:"sourceURL,attr"`
	Range     string `xml:"range,attr"`
}

type segListXML struct {
	Timescale string   `xml:"timescale,attr"`
	Duration  string   `xml:"duration,attr"`
	Init      *initXML `xml:"Initialization"`
	URLs      []struct {
		Media      string `xml:"media,attr"`
		MediaRange string `xml:"mediaRange,attr"`
	} `xml:"SegmentURL"`
}

// ParseDASH parses an MPD; base is the URL it was fetched from, used to
// resolve relative BaseURLs and segment URLs.
//
// Gotchas handled:
//   - BaseURL cascades: MPD → Period → AdaptationSet → Representation, each
//     resolved against the one above. When several are given (mirrors),
//     the first is used.
//   - SegmentTemplate attributes are inherited and overridden level by
//     level, the same way.
//   - Template identifiers may carry a printf width ($Number%05d$), and
//     "$$" is a literal dollar sign.
//   - In a SegmentTimeline, r="-1" repeats until the end of the Period.
//   - Ranges in the MPD are inclusive "first-last".
func ParseDASH(base *url.URL, data []byte) (*DASHManifest, error) {
	var m mpdXML
	if err := xml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("media: unreadable MPD: %w", err)
	}
	if len(m.Periods) == 0 {
		return nil, errors.New("media: MPD has no Period")
	}
	out := &DASHManifest{Live: m.Type == "dynamic"}
	if m.Duration != "" {
		d, err := parseISODuration(m.Duration)
		if err != nil {
			return nil, err
		}
		out.Duration = d
	}
	mpdBase := withBase(base, m.BaseURLs)
	budget := maxSegments

	for i, p := range m.Periods {
		period := DASHPeriod{ID: p.ID}
		if p.Start != "" {
			d, err := parseISODuration(p.Start)
			if err != nil {
				return nil, err
			}
			period.Start = d
		} else if i > 0 {
			prev := out.Periods[i-1]
			period.Start = prev.Start + prev.Duration
		}
		switch {
		case p.Duration != "":
			d, err := parseISODuration(p.Duration)
			if err != nil {
				return nil, err
			}
			period.Duration = d
		case i+1 < len(m.Periods) && m.Periods[i+1].Start != "":
			next, err := parseISODuration(m.Periods[i+1].Start)
			if err != nil {
				return nil, err
			}
			period.Duration = next - period.Start
		case out.Duration > 0:
			period.Duration = out.Duration - period.Start
		}

		periodBase := withBase(mpdBase, p.BaseURLs)
		for _, as := range p.Sets {
			setBase := withBase(periodBase, as.BaseURLs)
			for _, rx := range as.Reps {
				rep := DASHRepresentation{
					ID:          rx.ID,
					MimeType:    first(rx.MimeType, as.MimeType),
					ContentType: first(rx.ContentType, as.ContentType),
					Codecs:      first(rx.Codecs, as.Codecs),
					Lang:        as.Lang,
					Bandwidth:   atoi64(rx.Bandwidth),
					FrameRate:   first(rx.FrameRate, as.FrameRate),
					Protected:   len(rx.Protection)+len(as.Protection) > 0,
				}
				rep.Width, _ = strconv.Atoi(first(rx.Width, as.Width))
				rep.Height, _ = strconv.Atoi(first(rx.Height, as.Height))
				repBase := withBase(setBase, rx.BaseURLs)
				// Without any BaseURL the base is the MPD's own URL, which
				// is not media.
				explicit := hasURL(m.BaseURLs) || hasURL(p.BaseURLs) || hasURL(as.BaseURLs) || hasURL(rx.BaseURLs)
				x := expander{rep: &rep, base: repBase, explicitBase: explicit, period: period, live: out.Live, budget: &budget}
				if err := x.expand(p.segmentInfo, as.segmentInfo, rx.segmentInfo); err != nil {
					return nil, fmt.Errorf("media: representation %q: %w", rx.ID, err)
				}
				period.Representations = append(period.Representations, rep)
			}
		}
		out.Periods = append(out.Periods, period)
	}
	return out, nil
}

// expander fills one Representation's Init and Segments.
type expander struct {
	rep          *DASHRepresentation
	base         *url.URL
	explicitBase bool // a BaseURL was given at some level
	period       DASHPeriod
	live         bool
	budget       *int // segments left for the whole manifest
}

// expand uses the most specific segment description among the levels
// (Period, AdaptationSet, Representation), merging inherited attributes.
func (x expander) expand(levels ...segmentInfo) error {
	var tmpl *segTemplateXML
	var list *segListXML
	for _, l := range levels { // outermost first; inner levels override
		if l.Template != nil {
			tmpl = mergeTemplate(tmpl, l.Template)
		}
		if l.List != nil {
			list = mergeList(list, l.List)
		}
	}
	switch {
	case tmpl != nil:
		return x.template(tmpl)
	case list != nil:
		return x.list(list)
	default:
		// SegmentBase, or nothing at all: the Representation is one file.
		// Its index and init live inside that file, so fetching it whole
		// is enough and SegmentBase itself needs no parsing.
		if x.base == nil || !x.explicitBase {
			return errors.New("no media: neither segment information nor a BaseURL")
		}
		return x.add(DASHSegment{URL: x.base.String(), Duration: x.period.Duration})
	}
}

// add appends a segment, charging both limits.
func (x expander) add(seg DASHSegment) error {
	if err := x.room(); err != nil {
		return err
	}
	*x.budget--
	x.rep.Segments = append(x.rep.Segments, seg)
	return nil
}

func mergeTemplate(outer, inner *segTemplateXML) *segTemplateXML {
	if outer == nil {
		c := *inner
		return &c
	}
	c := *outer
	set := func(dst *string, v string) {
		if v != "" {
			*dst = v
		}
	}
	set(&c.Media, inner.Media)
	set(&c.Initialization, inner.Initialization)
	set(&c.StartNumber, inner.StartNumber)
	set(&c.Timescale, inner.Timescale)
	set(&c.Duration, inner.Duration)
	set(&c.PTO, inner.PTO)
	if inner.Timeline != nil {
		c.Timeline = inner.Timeline
	}
	return &c
}

// mergeList inherits SegmentList attributes and Initialization the same
// way; the SegmentURLs come from the innermost level that has any.
func mergeList(outer, inner *segListXML) *segListXML {
	if outer == nil {
		c := *inner
		return &c
	}
	c := *outer
	if inner.Timescale != "" {
		c.Timescale = inner.Timescale
	}
	if inner.Duration != "" {
		c.Duration = inner.Duration
	}
	if inner.Init != nil {
		c.Init = inner.Init
	}
	if len(inner.URLs) > 0 {
		c.URLs = inner.URLs
	}
	return &c
}

func (x expander) template(t *segTemplateXML) error {
	rep, base, period := x.rep, x.base, x.period
	if t.Media == "" {
		return errors.New("SegmentTemplate without media")
	}
	timescale := atoi64(t.Timescale)
	if timescale <= 0 {
		timescale = 1
	}
	number := int64(1)
	if t.StartNumber != "" {
		number = atoi64(t.StartNumber)
	}
	toDur := func(units int64) time.Duration {
		return time.Duration(float64(units) / float64(timescale) * float64(time.Second))
	}
	if t.Initialization != "" {
		u, err := fillTemplate(t.Initialization, rep, 0, 0)
		if err != nil {
			return err
		}
		rep.Init = &DASHSegment{URL: resolveURL(base, u)}
	}
	add := func(num, tm, d int64) error {
		if err := x.room(); err != nil { // before formatting: a hostile r is cheap to stop
			return err
		}
		u, err := fillTemplate(t.Media, rep, num, tm)
		if err != nil {
			return err
		}
		return x.add(DASHSegment{URL: resolveURL(base, u), Duration: toDur(d)})
	}

	if t.Timeline != nil {
		// Period end in timescale units, for r="-1"; 0 when unknown.
		var end int64
		if period.Duration > 0 {
			units, ok := toUnits(period.Duration.Seconds(), timescale)
			if !ok || units > math.MaxInt64-atoi64(t.PTO) {
				return errors.New("timescale out of range")
			}
			end = atoi64(t.PTO) + units
		}
		var tm int64
		for i, s := range t.Timeline.S {
			if s.T != "" {
				tm = atoi64(s.T)
			}
			d := atoi64(s.D)
			if d <= 0 {
				return fmt.Errorf("timeline entry %d has no duration", i)
			}
			r := atoi64(s.R)
			if r < 0 {
				// Repeat until the next entry's start, or the period end.
				limit := end
				if i+1 < len(t.Timeline.S) && t.Timeline.S[i+1].T != "" {
					limit = atoi64(t.Timeline.S[i+1].T)
				}
				switch {
				case limit == 0 && x.live:
					r = 0 // "until now": the next reload brings the rest
				case limit == 0:
					return errors.New(`r="-1" but the period length is unknown`)
				case limit <= tm:
					r = 0
				default:
					r = (limit-tm+d-1)/d - 1
				}
			}
			if err := x.reserve(r + 1); err != nil {
				return err
			}
			for k := int64(0); k <= r; k++ {
				if err := add(number, tm, d); err != nil {
					return err
				}
				number++
				tm += d
			}
		}
		return nil
	}

	dur := atoi64(t.Duration)
	if dur <= 0 {
		return errors.New("SegmentTemplate has neither SegmentTimeline nor duration")
	}
	if x.live {
		return nil // depends on availabilityStartTime and the clock
	}
	if period.Duration <= 0 {
		return errors.New("period length unknown, cannot count segments")
	}
	n := math.Ceil(period.Duration.Seconds() * float64(timescale) / float64(dur))
	if math.IsNaN(n) || n > maxSegmentsPerRep {
		return fmt.Errorf("representation expands to more than %d segments", maxSegmentsPerRep)
	}
	count := int64(n)
	if err := x.reserve(count); err != nil {
		return err
	}
	for k := int64(0); k < count; k++ {
		if err := add(number+k, k*dur, dur); err != nil {
			return err
		}
	}
	return nil
}

func (x expander) list(l *segListXML) error {
	rep, base := x.rep, x.base
	if l.Init != nil {
		seg, err := listSegment(base, l.Init.SourceURL, l.Init.Range)
		if err != nil {
			return err
		}
		rep.Init = &seg
	}
	timescale := atoi64(l.Timescale)
	if timescale <= 0 {
		timescale = 1
	}
	d := time.Duration(float64(atoi64(l.Duration)) / float64(timescale) * float64(time.Second))
	for _, su := range l.URLs {
		seg, err := listSegment(base, su.Media, su.MediaRange)
		if err != nil {
			return err
		}
		seg.Duration = d
		if err := x.add(seg); err != nil {
			return err
		}
	}
	if len(rep.Segments) == 0 {
		return errors.New("SegmentList without SegmentURL")
	}
	return nil
}

// listSegment builds a segment from a URL attribute (empty: the BaseURL
// itself) and an optional "first-last" range.
func listSegment(base *url.URL, ref, rng string) (DASHSegment, error) {
	seg := DASHSegment{URL: resolveURL(base, ref)}
	if rng != "" {
		r, err := parseDASHRange(rng)
		if err != nil {
			return seg, err
		}
		seg.Range = &r
	}
	return seg, nil
}

func parseDASHRange(s string) (ByteRange, error) {
	a, b, ok := strings.Cut(strings.TrimSpace(s), "-")
	first, err1 := strconv.ParseInt(a, 10, 64)
	last, err2 := strconv.ParseInt(b, 10, 64)
	// last == MaxInt64 would overflow the length.
	if !ok || err1 != nil || err2 != nil || first < 0 || last < first || last == math.MaxInt64 {
		return ByteRange{}, fmt.Errorf("bad range %q", s)
	}
	return ByteRange{Offset: first, Length: last - first + 1}, nil
}

// fillTemplate substitutes $RepresentationID$, $Bandwidth$, $Number$ and
// $Time$ (each optionally with a %0Nd width) and $$.
func fillTemplate(t string, rep *DASHRepresentation, number, tm int64) (string, error) {
	var b strings.Builder
	for {
		i := strings.IndexByte(t, '$')
		if i < 0 {
			b.WriteString(t)
			return b.String(), nil
		}
		b.WriteString(t[:i])
		t = t[i+1:]
		j := strings.IndexByte(t, '$')
		if j < 0 {
			return "", fmt.Errorf("unterminated identifier in template")
		}
		ident := t[:j]
		t = t[j+1:]
		name, format, _ := strings.Cut(ident, "%")
		var val string
		switch name {
		case "":
			val = "$"
		case "RepresentationID":
			val = rep.ID
		case "Bandwidth":
			val = formatInt(rep.Bandwidth, format)
		case "Number":
			val = formatInt(number, format)
		case "Time":
			val = formatInt(tm, format)
		default:
			return "", fmt.Errorf("unknown template identifier $%s$", ident)
		}
		b.WriteString(val)
	}
}

// formatInt applies a DASH width tag such as "05d"; anything else prints
// the plain number.
func formatInt(n int64, format string) string {
	s := strconv.FormatInt(n, 10)
	if format == "" {
		return s
	}
	f := strings.TrimSuffix(format, "d")
	f = strings.TrimPrefix(f, "0")
	width, err := strconv.Atoi(f)
	if err != nil || width <= len(s) || width > 32 {
		return s
	}
	return strings.Repeat("0", width-len(s)) + s
}

// withBase resolves the first of urls against base; with none it keeps base.
func withBase(base *url.URL, urls []string) *url.URL {
	for _, u := range urls {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		if base == nil {
			if p, err := url.Parse(u); err == nil {
				return p
			}
			continue
		}
		if p, err := base.Parse(u); err == nil {
			return p
		}
	}
	return base
}

func resolveURL(base *url.URL, ref string) string {
	if ref == "" {
		if base == nil {
			return ""
		}
		return base.String()
	}
	return resolve(base, ref)
}

// room reports whether one more segment fits under both limits.
func (x expander) room() error {
	if len(x.rep.Segments) >= maxSegmentsPerRep {
		return fmt.Errorf("representation expands to more than %d segments", maxSegmentsPerRep)
	}
	if *x.budget <= 0 {
		return fmt.Errorf("manifest expands to more than %d segments", maxSegments)
	}
	return nil
}

// reserve fails early when n more segments cannot fit, so an oversized
// template is refused before any of it is built. r is untrusted, hence the
// checks before adding.
func (x expander) reserve(n int64) error {
	if n < 0 || n > maxSegmentsPerRep || int64(len(x.rep.Segments))+n > maxSegmentsPerRep {
		return fmt.Errorf("representation expands to more than %d segments", maxSegmentsPerRep)
	}
	if n > int64(*x.budget) {
		return fmt.Errorf("manifest expands to more than %d segments", maxSegments)
	}
	return nil
}

// toUnits converts seconds to timescale units, refusing results an int64
// cannot hold (a hostile timescale such as 9e18).
func toUnits(sec float64, timescale int64) (int64, bool) {
	v := sec * float64(timescale)
	if math.IsNaN(v) || v < 0 || v >= math.MaxInt64 {
		return 0, false
	}
	return int64(v), true
}

func hasURL(urls []string) bool {
	for _, u := range urls {
		if strings.TrimSpace(u) != "" {
			return true
		}
	}
	return false
}

func first(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// parseISODuration reads the xs:duration subset MPDs use: PnDTnHnMnS, with
// fractional seconds. Years and months have no fixed length and are
// rejected unless zero (some encoders write "P0Y0M0DT0H3M30.000S").
func parseISODuration(s string) (time.Duration, error) {
	orig := s
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "P") {
		return 0, fmt.Errorf("media: bad duration %q", orig)
	}
	s = s[1:]
	var total float64
	inTime, parts := false, 0
	for len(s) > 0 {
		if s[0] == 'T' {
			inTime = true
			s = s[1:]
			continue
		}
		i := strings.IndexAny(s, "YMWDHS")
		if i <= 0 {
			return 0, fmt.Errorf("media: bad duration %q", orig)
		}
		v, err := strconv.ParseFloat(s[:i], 64)
		if err != nil || v < 0 {
			return 0, fmt.Errorf("media: bad duration %q", orig)
		}
		switch unit := s[i]; {
		case unit == 'D' && !inTime:
			total += v * 86400
		case unit == 'W' && !inTime:
			total += v * 7 * 86400
		case unit == 'H' && inTime:
			total += v * 3600
		case unit == 'M' && inTime:
			total += v * 60
		case unit == 'S' && inTime:
			total += v
		case (unit == 'Y' || unit == 'M') && !inTime && v == 0:
		default: // non-zero years or months
			return 0, fmt.Errorf("media: unsupported duration %q", orig)
		}
		s = s[i+1:]
		parts++
	}
	if parts == 0 { // "P" or "PT" alone is not a duration
		return 0, fmt.Errorf("media: bad duration %q", orig)
	}
	d, ok := seconds(total)
	if !ok {
		return 0, fmt.Errorf("media: bad duration %q", orig)
	}
	return d, nil
}
