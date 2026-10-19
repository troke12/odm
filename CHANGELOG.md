# Changelog

All notable changes to ODM are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/) and the project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- A connection that receives no bytes for 30 seconds is closed and its chunk
  is retried from the last written offset. The wait for response headers
  has its own, longer limit of 5 minutes, for servers that build the file
  before they answer. Other connections of the same file keep going.

### Fixed

- With ODM not running, the browser extension started a native host for
  every video it noticed, which on Windows left hundreds of `cmd.exe` and
  `conhost.exe` processes behind (#14). A failed settings fetch is now
  retried after 30 seconds, requests arriving together share one fetch,
  and a page past its peek budget no longer asks at all.
- Range requests send `Accept-Encoding: identity`, also over an
  `Accept-Encoding` set on the download, so a server that compresses
  without being asked cannot have compressed bytes written into the file.
  Go already left out gzip on range requests; this covers the server side.

## [0.4.2] - 2026-10-16

### Added

- When the window stops responding for 5 seconds, ODM writes where it is
  stuck to `odm-hang-<time>.txt` in its settings folder, to attach to a
  bug report.

### Changed

- The main window's title shows the version ("Ochi Download Manager
  0.4.2").
- The menu bar on Windows and Linux is ODM's own: titles with room around
  them and a highlight under the pointer, and roomier drop-down menus with
  a lifted background. With a menu open, pointing at another title opens
  that one, and the arrow keys move within and between menus. macOS keeps
  its system menu bar. View offers "Show categories" while the sidebar is
  hidden.

### Fixed

- Removing a download no longer freezes the window ("Not Responding"):
  Windows can take seconds to delete a large partial file, and that ran
  on the UI thread. Removal now runs in the background and the download
  leaves the list at once.
- Toolbar buttons and the list's column headers highlight under the
  pointer again (their hover code never ran), and a toolbar button looks
  pushed in while it is held.
- Selecting a download with a click is immediate; it used to wait about a
  third of a second, as the list waited to see whether a double click
  followed. Double-clicking still opens the progress window.
- The pointer highlights the whole row in the download list, not just
  the cell under it.
- The hover highlight in the category sidebar no longer vanishes within a
  second: the list's refresh every second cleared it, so the sidebar is
  now refreshed only when it changes. Its rows also no longer shift a few
  pixels when the pointer touches them: they are laid out with fixed
  spacing instead of the theme's.
- On Windows, a window moved to a monitor with another display scale
  (100% and 200%, say) is redrawn at that scale; its content used to keep
  the old one and looked tiny or huge. This works around Fyne recomputing
  the scale before Windows reports the new one.
- The download list's last column (Description) reaches the right edge:
  its header and the selected row stopped a few pixels short, and a stray
  grid line stood in the gap.

## [0.4.1] - 2026-10-16

### Security

- File downloads get the protection streams got in 0.4.0: a link on a
  public server that redirects to the user's own machine or network
  (localhost, a router, a NAS) is refused, and so is the New Download
  window's look at it. Links that are themselves on the local network
  still work. The site grabber is held to its start page the same way:
  crawling a public site no longer fetches or queues pages and files on
  the local network that it links to.

### Fixed

- File names drop control characters and invalid UTF-8, and a name
  shortened to fit keeps its extension and no longer cuts a character in
  two.
- The extension keeps a tab's media list when only the page's `#hash`
  changes; any other change of address still starts a fresh list.

## [0.4.0] - 2026-10-15

### Added

- The extension's toolbar icon shows how many videos, audio files and
  streams the page has loaded, and its popup lists them (from every frame,
  with how long ago each was seen) to send one to ODM. This covers pages
  where the in-page button does not appear.
- The extension's menu and popup were redrawn: each item shows its kind,
  what it holds ("All qualities, up to 1080p", "One quality" plus host and
  file) and LIVE and Best badges; the playlist with every quality is
  listed first. To know this the extension reads a stream's playlist once
  (at most 8 per page, not in private windows, Firefox containers or
  excluded sites); when that fails the label shows the host and file name.
  The popup follows the browser's light or dark theme.
- Live HLS streams are recorded: ODM keeps reloading the playlist and
  saving new segments until the download is stopped (Pause or Stop
  finishes the file), the broadcast ends, or nothing new arrives for a
  while. A segment that cannot be fetched before it leaves the playlist is
  skipped. If ODM closes mid-way, Resume continues the recording after a
  gap. The New Download window marks such a stream as live. Live DASH is
  still refused. The progress window counts segments recorded,
  downloading and skipped, and warns when the connection is slower than
  the stream plays, so a lower quality should be picked.
- A stream made of MPEG-TS segments (most HLS) is saved as MP4 when ffmpeg
  is found, remuxed without re-encoding; MP4 opens and seeks in more
  players. Without ffmpeg, or if the remux fails, it stays a `.ts` file
  and the progress window says why.

### Security

- A stream whose playlist is on a public server can no longer make ODM
  request addresses on the user's own machine or network (localhost,
  private and link-local ranges, including after a redirect). Browsers
  block such requests from web pages; ODM fetched them on the page's
  behalf. Playlists that are themselves on the local network still work.
  A public name that resolves to a private address (some company networks
  do this for caches) is refused as well.

### Fixed

- File names that Windows reserves for devices (`CON`, `NUL`, `COM1`, …,
  with any extension) get a leading `_`. Before, a page titled "nul" or
  "con" could make the download fail or write into a device on Windows.

### Changed

- The extension sends a media address as the page requested it, without
  removing any site's byte-range parameters; only the `#fragment` and the
  low-latency HLS reload parameters (`_HLS_msn`, `_HLS_part`, `_HLS_skip`)
  are dropped, leaving the rest of the query byte for byte.

## [0.3.1] - 2026-10-15

### Fixed

- A paused or failed download closes its partial file. Resuming it in the
  same session no longer leaves an earlier handle open, which on Windows
  made the final rename fail ("being used by another process"). This also
  affected HLS and DASH streams. (#12, by @rigaz29)

## [0.3.0] - 2026-10-15

### Added

- HLS and DASH streams: ODM reads the playlist, picks the best quality,
  downloads the segments over several connections (each segment is kept,
  so a resume skips the finished ones) and joins them into one file.
  AES-128 encrypted HLS is decrypted. Separate video and audio tracks are
  joined with ffmpeg when it is found next to ODM, on PATH, or at the path
  chosen in Options > Downloads, which shows the version ODM found. Expired links are refreshed once from
  the playlist. Live and DRM-protected streams are refused.
- The extension's "Download this video" button now sends HLS/DASH streams
  to ODM.
- The New Download window reads a stream's playlist and offers its
  qualities (resolution, bitrate, codecs, estimated size) and audio tracks,
  best first. This also works for a playlist address typed or pasted by
  hand. A single-quality playlist says so, and live or DRM-protected
  streams are refused before anything is queued; if the playlist cannot be
  read just then, the stream can still be queued and ODM picks the best
  quality when it runs.
- The progress window of a stream counts its segments ("12 of 340")
  instead of showing an empty connection map.

### Changed

- Removing a download together with its file waits for the transfer to
  stop first, so the file is not still open (Windows refused to delete it).

## [0.2.0] - 2026-10-13

### Added

- "Download this video" button over video players: the extension notes video
  and audio files the page loads (512 KB and up, Google video byte-range
  requests turned back into the whole file) and hands the chosen one to ODM
  named after the page title (site name after the last " | " dropped,
  spaces as underscores: `Episode_11.mp4`). HLS/DASH streams are listed as
  unsupported.
- Downloads handed over by the browser open a New Download window (its own
  window, so it works while ODM is in the tray) prefilled with the name,
  cookies, referrer and user agent, which are also used for the size probe.
  *Options → Downloads* can turn it off.
- The extension reads its file types and a new "leave these sites alone"
  list from ODM's Options; Alt-click lets the browser download a link.

- Releases include the browser extension: `odm-extension-chromium.zip` for
  Chrome, Edge, Brave and Opera (load unpacked) and a Mozilla-signed
  `odm-extension-firefox.xpi` (unsigned on pre-releases).
- `cmd/extpack` derives a manifest per browser from `extension/`.

### Changed

- The Firefox add-on ID is now `odm@troke.id`, and the add-on declares the
  data it hands to ODM (Firefox's data collection permissions).
- The native host allows a list of Chromium extension IDs, ready for store
  IDs alongside the pinned unpacked ID.

### Fixed

- A download the extension took over was lost when ODM was closed or not
  registered: the browser's copy was cancelled before ODM confirmed. It is
  now paused, then cancelled only after ODM accepts, and resumed otherwise.
- The Add dialog probed the size without the download's headers, so sites
  needing cookies or a referrer showed an error instead of the size.
- The extension did not run in Firefox at all: its manifest only had
  `background.service_worker`, which Firefox does not support. The Firefox
  package uses `background.scripts`.

## [0.1.1] - 2026-10-13

### Added

- Refresh download address: point an unfinished download at a new URL when
  its link expires. ODM checks that the new address serves the same file
  (same size, resumable once bytes are on disk) before switching.
- Duplicate links: the Add dialog asks whether to show the existing entry or
  download again; browser, clipboard, grabber and API adds follow a setting
  (skip, default, or download again).
- Clipboard monitor on Linux and macOS (desktop build).
- Release archives for Linux ARM64 and macOS Intel.

### Changed

- The clipboard monitor's on/off switch applies immediately instead of after
  a restart. A link already on the clipboard when ODM starts, or when the
  monitor is switched on, is not grabbed.

### Fixed

- A new download no longer replaces an existing file with the same name;
  finishing used to delete it. Downloads now claim a free name
  (`name (2).ext`), and two downloads of the same name no longer share one
  partial file.
- The Windows clipboard reader is bounded by the clipboard block's real size.

## [0.1.0] - 2026-10-13

### Added

- Segmented, resumable download engine: multiple sockets per file using
  `Range: bytes=`, dynamic chunk splitting, per-chunk resume state, retry with
  backoff, shared global and per-download speed limits.
- Graceful single-stream fallback for servers without `Accept-Ranges`.
- Category folders by file type, automatic subfolder placement.
- Queues: named queues, `Start queue` / `Stop queue`, move a download between
  queues, `Q` column. A started queue runs one download at a time in queue
  order; items can be moved up and down.
- Scheduler: start/stop queues at set times on chosen weekdays; optionally
  exit ODM or shut the computer down when started queues finish, after a
  cancellable 30-second countdown.
- Extra file types for the clipboard monitor and site grabber; per-category
  destination folders.
- Site logins: per-host credentials applied as HTTP Basic auth.
- HTTP proxy support.
- Download list export / import as JSON.
- Site grabber: breadth-first crawl with depth/page/file limits that queues
  every recognised file link.
- Browser takeover: MV3 extension plus a native-messaging relay host.
- Windows clipboard monitor for downloadable URLs.
- Desktop UI (Fyne, Windows/macOS/Linux with the `fyne` build tag): menu bar,
  icon toolbar, category/queue sidebar, sortable report table, Add/Options/
  progress/grabber dialogs.
- Headless mode: JSON + SSE API and an embedded web UI (`-no-ui`).
- Progress window opens when a download is started or resumed (status,
  speed limiter, completion options, connection map); completion window
  with Open / Open folder when a download finishes.
- System tray: close to tray, tray menu, launch on startup (hidden), and a
  second launch brings the running window forward.
- Multi-select in the list (Ctrl/Shift-click, Ctrl+A) with Delete, Enter,
  arrow keys and Ctrl+N shortcuts.
- Scheduler window with per-queue schedules (start with ODM, at a time once
  or on weekdays, stop at a time, retries, open file / exit / shut down when
  done, files at the same time) and download limits.
- Options window with File types, Save to, Downloads, Connection (per-server
  connection exceptions, download limits), Proxy / Socks (system, manual
  HTTP or SOCKS5) and Site logins tabs; configurable User-Agent.
- `-register-browser` and an Options button register the native host for
  Chrome, Edge, Chromium and Firefox; the extension's ID is pinned.
- Update check against GitHub Releases at start-up and from the Help menu
  (notify only, can be disabled in Options).
- Local control channel over a Unix domain socket (browser relay, single
  instance); no TCP port is opened unless `-serve` is given, and that
  listener requires a per-run token and rejects foreign origins.

[Unreleased]: https://github.com/troke12/odm/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/troke12/odm/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/troke12/odm/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/troke12/odm/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/troke12/odm/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/troke12/odm/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/troke12/odm/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/troke12/odm/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/troke12/odm/releases/tag/v0.1.0
