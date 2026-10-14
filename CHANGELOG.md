# Changelog

All notable changes to ODM are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/) and the project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Connections open one at a time. The next connection starts after every
  connection already open has stored file bytes, up to the configured limit.

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

[Unreleased]: https://github.com/troke12/odm/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/troke12/odm/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/troke12/odm/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/troke12/odm/releases/tag/v0.1.0
