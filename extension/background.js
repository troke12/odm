// ODM Integration: hands browser downloads and detected videos to Ochi
// Download Manager through native messaging. The pure helpers at the top
// carry no browser state, so extension/test/ runs them under Node.

const HOST = "com.ochi.odm";

// Used until ODM answers a "config" message (ODM not running yet, or an
// older ODM): the same built-in list ODM ships with.
const DEFAULT_TYPES = [
  "3gp", "7z", "aac", "aiff", "apk", "appx", "avi", "bat", "bmp", "bz2", "csv", "deb", "dmg",
  "doc", "docx", "epub", "exe", "flac", "flv", "gif", "gz", "heic", "ico", "img", "iso", "jpeg",
  "jpg", "m4a", "m4v", "mkv", "mov", "mp3", "mp4", "mpeg", "mpg", "msi", "mts", "odt", "ogg",
  "opus", "pdf", "pkg", "png", "ppt", "pptx", "rar", "rpm", "rtf", "sh", "svg", "tar", "tiff",
  "ts", "txt", "vhd", "vob", "wav", "webm", "webp", "wma", "wmv", "xls", "xlsx", "xz", "zip", "zst",
];

// Smaller media responses are ads, previews or interface sounds.
const MIN_MEDIA_BYTES = 512 * 1024;
const CONFIG_TTL_MS = 5 * 60 * 1000;
// How long a failed config fetch stands before ODM is asked again. Every
// ask starts a native host process (on Windows through cmd.exe), so with
// ODM not running, retrying on every event spawned hosts without limit.
const CONFIG_RETRY_MS = 30 * 1000;
const MAX_MEDIA_PER_TAB = 15;
const BYPASS_MS = 10 * 1000;

const STREAM_TYPES = [
  "application/vnd.apple.mpegurl", "application/x-mpegurl", "audio/mpegurl", "audio/x-mpegurl",
  "application/dash+xml",
];
const MEDIA_EXT = /\.(mp4|m4v|webm|mkv|mov|flv|mp3|m4a|aac|ogg|opus|flac|wav)$/i;

/* ---------- pure helpers ---------- */

function urlExt(url) {
  try {
    const m = /\.([a-z0-9]{1,8})$/i.exec(new URL(url).pathname);
    return m ? m[1].toLowerCase() : "";
  } catch {
    return "";
  }
}

// hasType reports whether url (or the browser's file name) ends in one of
// types, given without dots.
function hasType(url, types, filename) {
  const set = new Set(types.map((t) => String(t).toLowerCase().replace(/^\./, "")));
  const fromName = /\.([a-z0-9]{1,8})$/i.exec(filename || "");
  return set.has(urlExt(url)) || (fromName !== null && set.has(fromName[1].toLowerCase()));
}

// configFresh reports whether a cached config may still be used: ODM's
// answer for CONFIG_TTL_MS, the built-in fallback (ok false) for the
// shorter CONFIG_RETRY_MS.
function configFresh(cfg, now) {
  if (!cfg || typeof cfg.at !== "number") return false;
  return now - cfg.at < (cfg.ok ? CONFIG_TTL_MS : CONFIG_RETRY_MS);
}

// hostMatches: "example.org" matches that host; "*.example.org" matches it
// and every subdomain.
function hostMatches(host, pattern) {
  host = String(host).toLowerCase();
  pattern = String(pattern).toLowerCase().trim();
  if (!pattern) return false;
  if (pattern.startsWith("*.")) {
    const base = pattern.slice(2);
    return host === base || host.endsWith("." + base);
  }
  return host === pattern;
}

function isExcluded(url, patterns) {
  try {
    const host = new URL(url).hostname;
    return (patterns || []).some((p) => hostMatches(host, p));
  } catch {
    return false;
  }
}

// mediaKind classifies a response: "stream" for HLS/DASH playlists (ODM
// fetches their segments), "file" for a single audio/video file, null for
// anything else.
function mediaKind(url, mime) {
  mime = String(mime || "").split(";")[0].trim().toLowerCase();
  let path = "";
  try {
    path = new URL(url).pathname.toLowerCase();
  } catch {
    return null;
  }
  if (STREAM_TYPES.includes(mime) || path.endsWith(".m3u8") || path.endsWith(".mpd")) return "stream";
  // Segments of a stream, not whole files: MPEG-TS (HLS) and fragmented
  // MP4 (DASH, newer HLS). Listing them would also push the playlists out.
  if (mime === "video/mp2t" || path.endsWith(".ts")) return null;
  if (mime === "video/iso.segment" || mime === "audio/iso.segment" || path.endsWith(".m4s")) return null;
  if (mime.startsWith("video/") || mime.startsWith("audio/")) return "file";
  if (MEDIA_EXT.test(path) && (mime === "" || mime === "application/octet-stream")) return "file";
  return null;
}

// streamFormat names a stream's format for the menu: "HLS", "DASH" or ""
// when the type and URL don't say. ODM decides again on its side.
function streamFormat(url, mime) {
  mime = String(mime || "").split(";")[0].trim().toLowerCase();
  let path = "";
  try {
    path = new URL(url).pathname.toLowerCase();
  } catch {
    return "";
  }
  if (mime === "application/dash+xml" || path.endsWith(".mpd")) return "DASH";
  if (STREAM_TYPES.includes(mime) || path.endsWith(".m3u8")) return "HLS";
  return "";
}

// normalizeMediaURL drops the fragment, which the server never sees, so
// the same file reached as v.mp4 and v.mp4#t=10 is listed once, and the
// low-latency HLS reload parameters (_HLS_msn, _HLS_part, _HLS_skip, from
// the HLS specification, RFC 8216bis): a player adds new ones on every
// reload of the same playlist. Other query parameters are kept: no site's
// parameters are special.
function normalizeMediaURL(url) {
  try {
    const u = new URL(url);
    u.hash = "";
    // Cut those pieces out of the raw query: URLSearchParams would write
    // every other parameter back re-encoded, which breaks signatures
    // computed over the query as it was.
    if (u.search.includes("_HLS_")) {
      const kept = u.search.slice(1).split("&").filter((p) => !p.startsWith("_HLS_"));
      u.search = kept.length ? "?" + kept.join("&") : "";
    }
    return u.toString();
  } catch {
    return url;
  }
}

// totalSize reads the full size from Content-Range ("bytes 0-99/1234") or,
// for a whole response, Content-Length. 0 when unknown.
function totalSize(headers) {
  const get = (name) => {
    const h = (headers || []).find((x) => x.name.toLowerCase() === name);
    return h ? h.value : "";
  };
  const range = /\/(\d+)\s*$/.exec(get("content-range"));
  if (range) return Number(range[1]);
  const len = Number(get("content-length"));
  return Number.isFinite(len) && len > 0 ? len : 0;
}

// describePlaylist reads what a playlist offers from its text, for the
// label in the menu and popup: { master, quality, live }. master is true
// for an HLS master playlist or an MPD with several qualities (the one to
// pick: it lets ODM offer every quality and the separate audio); quality
// is like "All qualities, up to 1080p" or "One quality". truncated: only
// the start was read, so an end marker cannot be looked for. Returns null
// for text that is not a playlist of the given format.
//
// Gotcha: the text comes from any web page. Everything here is linear in
// its length; a pattern like /<Tag[^>]*attr/ backtracks over the rest of
// the text for every unclosed tag, which a hostile page can make cost
// tens of seconds on the extension's only thread.
function describePlaylist(text, format, truncated) {
  let master = false;
  let live = false;
  let quality = "";
  if (format === "HLS") {
    if (!text.trimStart().startsWith("#EXTM3U")) return null;
    let variants = 0;
    let height = 0;
    for (const line of text.split("\n")) {
      if (!line.startsWith("#EXT-X-STREAM-INF")) continue;
      variants++;
      const m = /RESOLUTION=\d{1,6}x(\d{1,6})/.exec(line);
      if (m) height = Math.max(height, Number(m[1]));
    }
    if (variants > 0) {
      master = true;
      quality = variants > 1 ? `All qualities${height ? `, up to ${height}p` : ""}` : `One quality${height ? `, ${height}p` : ""}`;
    } else {
      quality = "One quality";
      live = !truncated && !text.includes("#EXT-X-ENDLIST") && !/#EXT-X-PLAYLIST-TYPE:[ \t]*VOD/.test(text);
    }
  } else if (format === "DASH") {
    let isMPD = false;
    const heights = new Set();
    // Each piece between two "<" is one tag (up to its ">") plus text.
    for (const piece of text.split("<")) {
      const end = piece.indexOf(">");
      const tag = end >= 0 ? piece.slice(0, end) : piece;
      const name = /^[\w:.-]{0,64}/.exec(tag)[0].replace(/^.*:/, "");
      if (name === "MPD") {
        isMPD = true;
        live = live || /\btype=["']dynamic["']/.test(tag);
      } else if (name === "Representation" || name === "AdaptationSet") {
        const m = /\bheight=["'](\d{1,6})["']/.exec(tag);
        if (m) heights.add(Number(m[1]));
      }
    }
    if (!isMPD) return null;
    const max = Math.max(0, ...heights);
    master = heights.size > 1;
    quality = master ? `All qualities${max ? `, up to ${max}p` : ""}` : `One quality${max ? `, ${max}p` : ""}`;
  } else {
    return null;
  }
  return { master, quality, live };
}

// mergeMedia adds item to list (deduplicated by URL, keeping the larger
// known size and the first time it was seen), newest first, capped. Over
// the cap the oldest file goes first: a player keeps loading files, while
// the playlist with every quality is loaded once, at the start.
function mergeMedia(list, item) {
  const rest = (list || []).filter((x) => x.url !== item.url);
  const old = (list || []).find((x) => x.url === item.url);
  if (old) {
    item = { ...item, size: Math.max(old.size, item.size) };
    if (old.at !== undefined && !(item.at <= old.at)) item.at = old.at;
    for (const k of ["quality", "master", "live"]) {
      if (old[k] !== undefined && item[k] === undefined) item[k] = old[k];
    }
  }
  const next = [item, ...rest];
  while (next.length > MAX_MEDIA_PER_TAB) {
    const f = next.findLastIndex((x) => x.kind !== "stream");
    next.splice(f >= 0 ? f : next.length - 1, 1);
  }
  return next;
}

/* ---------- browser side ---------- */

function askOdm(message) {
  return new Promise((resolve) => {
    try {
      chrome.runtime.sendNativeMessage(HOST, message, (reply) => {
        if (chrome.runtime.lastError) {
          resolve({ ok: false, error: chrome.runtime.lastError.message });
        } else {
          resolve(reply || { ok: false, error: "no reply from ODM" });
        }
      });
    } catch (e) {
      resolve({ ok: false, error: String(e) });
    }
  });
}

async function isEnabled() {
  const { enabled = true } = await chrome.storage.local.get("enabled");
  return enabled;
}

// One config fetch at a time: events arriving together all read the cache
// before any of them writes it, and would each start a host. Kept in
// memory only, so it is gone with the service worker, as the fetch is.
let configPending = null;

async function getConfig() {
  const { odmConfig } = await chrome.storage.session.get("odmConfig");
  if (configFresh(odmConfig, Date.now())) return odmConfig;
  if (!configPending) {
    configPending = (async () => {
      const reply = await askOdm({ type: "config" });
      const ok = Boolean(reply.ok && reply.config);
      const cfg = ok
        ? { fileTypes: reply.config.fileTypes || DEFAULT_TYPES, excludeSites: reply.config.excludeSites || [], ok, at: Date.now() }
        : { fileTypes: DEFAULT_TYPES, excludeSites: [], ok, at: Date.now() }; // asked again after CONFIG_RETRY_MS
      await chrome.storage.session.set({ odmConfig: cfg });
      return cfg;
    })().finally(() => { configPending = null; });
  }
  return configPending;
}

async function cookieHeader(url) {
  try {
    const cookies = await chrome.cookies.getAll({ url });
    return cookies.map((c) => `${c.name}=${c.value}`).join("; ");
  } catch {
    return "";
  }
}

async function handOff(fields) {
  return askOdm({
    type: "download",
    cookies: await cookieHeader(fields.url),
    userAgent: navigator.userAgent,
    ...fields,
  });
}

const tabKey = (tabId) => `media:${tabId}`;

// sameDocument reports whether two URLs differ only in their #hash.
function sameDocument(a, b) {
  try {
    const x = new URL(a);
    const y = new URL(b);
    x.hash = "";
    y.hash = "";
    return x.href === y.href;
  } catch {
    return false;
  }
}

// saveTab stores a tab's list and shows it: badge, and the frame's button.
async function saveTab(tabId, frameId, list) {
  await chrome.storage.session.set({ [tabKey(tabId)]: list });
  setBadge(tabId, list.length);
  const forFrame = list.filter((x) => x.frameId === frameId);
  chrome.tabs.sendMessage(tabId, { type: "odm-media", items: forFrame }, { frameId })
    .catch(() => { /* no content script in that frame (yet) */ });
}

// Peeks per tab since its page loaded: a page that keeps loading new
// playlist addresses is not followed by a request for each.
const MAX_PEEKS_PER_PAGE = 8;
const peeks = new Map();

// mayPeek decides whether a stream may be fetched for its label. Not in a
// private window or a Firefox container: the background's fetch would use
// the default cookie store and link that browsing to the normal one. Not
// for sites the user excluded, and not beyond the per-page budget.
async function mayPeek(tabId, url) {
  try {
    const tab = await chrome.tabs.get(tabId);
    if (tab.incognito) return false;
    if (tab.cookieStoreId && tab.cookieStoreId !== "firefox-default") return false;
  } catch {
    return false;
  }
  // The budget is checked before the config too: getConfig may start a
  // native host, and a spent budget must not keep asking for one.
  if ((peeks.get(tabId) || 0) >= MAX_PEEKS_PER_PAGE) return false;
  const cfg = await getConfig();
  if (isExcluded(url, cfg.excludeSites)) return false;
  // Read again: other streams of this tab may have spent it meanwhile.
  const n = peeks.get(tabId) || 0;
  if (n >= MAX_PEEKS_PER_PAGE) return false;
  peeks.set(tabId, n + 1);
  return true;
}

// Limits for peekPlaylist: a master playlist or MPD lists its qualities
// at the start, while a long recorded playlist can run to megabytes.
const PEEK_BYTES = 512 * 1024;
const PEEK_MS = 5000;

// peekPlaylist fetches a playlist once more, as the page did (with its
// cookies), and describes it. null when it cannot be read: the label then
// falls back to host and file name. Gotcha: this is an extra request; a
// server that allows each URL only once would refuse ODM's own fetch of
// it later, which is why it is made only for streams, once per URL.
async function peekPlaylist(url, format) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), PEEK_MS);
  try {
    const resp = await fetch(url, { credentials: "include", cache: "no-store", signal: ctl.signal });
    if (!resp.ok || !resp.body) return null;
    const reader = resp.body.getReader();
    const chunks = [];
    let got = 0;
    let truncated = false;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      got += value.length;
      if (got >= PEEK_BYTES) {
        truncated = true;
        reader.cancel().catch(() => {});
        break;
      }
    }
    const bytes = new Uint8Array(got);
    let off = 0;
    for (const c of chunks) {
      bytes.set(c, off);
      off += c.length;
    }
    return describePlaylist(new TextDecoder().decode(bytes), format, truncated);
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// grab sends one detected media item to ODM. The referrer is the page (or
// frame) it was found on; streams get the page title as their name.
function grab(item, referrer, title) {
  return handOff({
    url: item.url, referrer, title,
    mime: item.mime || "", size: item.size || 0,
    kind: item.kind === "stream" ? "stream" : "file",
  });
}

// popupReferrer picks the referrer for an item sent from the popup, as
// close as possible to the frame URL the in-page button would send: the
// tab's URL for the top frame, else the frame's URL (Firefox) or origin
// (Chromium).
function popupReferrer(item, tab) {
  const tabURL = (tab && tab.url) || "";
  if (item.frameId === 0) return tabURL;
  return item.page || (item.origin ? item.origin + "/" : "") || tabURL;
}

// setBadge shows how many media items the tab has on the toolbar icon.
function setBadge(tabId, n) {
  try {
    const p = chrome.action.setBadgeText({ tabId, text: n ? String(n) : "" });
    if (p && p.catch) p.catch(() => { /* tab gone */ });
  } catch { /* tab gone */ }
}

async function tabMedia(tabId) {
  const key = tabKey(tabId);
  return (await chrome.storage.session.get(key))[key] || [];
}

// Players load several playlists at once; each update reads, merges and
// writes the tab's list, so updates to one tab run one after another or
// the later write drops what the earlier one added.
const tabQueues = new Map();
function updateTab(tabId, fn) {
  const run = (tabQueues.get(tabId) || Promise.resolve()).then(fn).catch(() => {});
  tabQueues.set(tabId, run);
  run.then(() => {
    if (tabQueues.get(tabId) === run) tabQueues.delete(tabId);
  });
  return run;
}

if (typeof chrome !== "undefined" && chrome.runtime && chrome.runtime.id) {
  // Listeners are registered synchronously at load, as both a Chromium
  // service worker and a Firefox event page require.

  chrome.downloads.onCreated.addListener(async (item) => {
    if (!(await isEnabled())) return;
    const url = item.finalUrl || item.url;
    if (!url || !/^https?:/.test(url)) return;
    const { bypass = {} } = await chrome.storage.session.get("bypass");
    if (bypass[url] && Date.now() - bypass[url] < BYPASS_MS) return; // Alt-click
    const cfg = await getConfig();
    if (isExcluded(url, cfg.excludeSites) || !hasType(url, cfg.fileTypes, item.filename)) return;

    // Hold the browser's copy until ODM confirms; it used to be cancelled
    // first, so with ODM closed or unregistered the download was lost.
    try { await chrome.downloads.pause(item.id); } catch { /* may already be done */ }
    const reply = await handOff({
      url, referrer: item.referrer || "", filename: item.filename || "",
      mime: item.mime || "", size: item.totalBytes > 0 ? item.totalBytes : 0,
    });
    if (reply.ok) {
      chrome.downloads.cancel(item.id, () => chrome.downloads.erase({ id: item.id }));
    } else {
      try { await chrome.downloads.resume(item.id); } catch { /* finished meanwhile */ }
    }
  });

  chrome.runtime.onInstalled.addListener(() => {
    chrome.contextMenus.create({
      id: "odm-download",
      title: "Download with ODM",
      contexts: ["link", "video", "audio", "image"],
    });
  });

  chrome.contextMenus.onClicked.addListener((info, tab) => {
    const url = info.linkUrl || info.srcUrl;
    if (url && /^https?:/.test(url)) {
      handOff({ url, referrer: info.pageUrl || "", title: (tab && tab.title) || "" });
    }
  });

  chrome.webRequest.onHeadersReceived.addListener(
    (d) => {
      if (d.tabId < 0 || (d.statusCode !== 200 && d.statusCode !== 206)) return;
      const ct = (d.responseHeaders || []).find((h) => h.name.toLowerCase() === "content-type");
      const mime = ct ? ct.value.split(";")[0].trim().toLowerCase() : "";
      const kind = mediaKind(d.url, mime);
      if (!kind) return;
      const size = totalSize(d.responseHeaders);
      if (kind === "file" && size > 0 && size < MIN_MEDIA_BYTES) return;
      const item = { url: normalizeMediaURL(d.url), mime, size, kind, frameId: d.frameId, at: Date.now() };
      // The document that asked for it, for the referrer when the popup
      // sends it: Firefox gives the frame's URL, Chromium only its origin.
      if (d.documentUrl) item.page = d.documentUrl;
      else if (d.initiator && d.initiator !== "null") item.origin = d.initiator;
      if (kind === "stream") item.format = streamFormat(d.url, mime);
      updateTab(d.tabId, async () => {
        const before = await tabMedia(d.tabId);
        const fresh = !before.some((x) => x.url === item.url);
        const next = mergeMedia(before, item);
        await saveTab(d.tabId, d.frameId, next);
        // A stream seen for the first time is read once to tell the
        // playlist with every quality from the others (a player reloads
        // the same live playlist URL every few seconds: not read again).
        // mayPeek may ask ODM for its settings: not awaited here, or a
        // slow reply would hold up every later update of this tab.
        if (fresh && kind === "stream") {
          mayPeek(d.tabId, item.url).then((ok) => (ok ? peekPlaylist(item.url, item.format) : null)).then((info) => {
            if (!info) return;
            updateTab(d.tabId, async () => {
              const list = await tabMedia(d.tabId);
              const at = list.findIndex((x) => x.url === item.url);
              if (at < 0) return; // gone: the page changed meanwhile
              list[at] = { ...list[at], ...info };
              await saveTab(d.tabId, d.frameId, list);
            });
          });
        }
      });
    },
    { urls: ["<all_urls>"], types: ["media", "xmlhttprequest", "object", "other"] },
    ["responseHeaders"],
  );

  // A new page in the tab starts a fresh media list, also one a script
  // switched to (pushState): its media is not the previous view's, and a
  // stale playlist would be offered first. Only a change of the #hash
  // keeps the list, as the page itself stays. tabURLs is in memory: after
  // the background restarts, the first change resets as before.
  const tabURLs = new Map();
  chrome.tabs.onUpdated.addListener((tabId, change) => {
    if (!change.url) return;
    const before = tabURLs.get(tabId);
    tabURLs.set(tabId, change.url);
    if (before && sameDocument(before, change.url)) return;
    // Queued like the updates, or one still running for the old page
    // could write its list (and count) back after the reset.
    peeks.delete(tabId);
    updateTab(tabId, async () => {
      await chrome.storage.session.remove(tabKey(tabId));
      setBadge(tabId, 0);
    });
  });
  chrome.tabs.onRemoved.addListener((tabId) => {
    tabURLs.delete(tabId);
    peeks.delete(tabId);
    chrome.storage.session.remove(tabKey(tabId));
  });

  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    const tab = sender.tab;
    if (!tab) {
      // The toolbar popup: not in a tab, so it names the tab it shows.
      // Content scripts in frames of tab-less extension pages also come
      // without a tab, so the sender must be our popup page itself.
      const popup = chrome.runtime.getURL("popup.html");
      if (sender.id !== chrome.runtime.id || !sender.url || !sender.url.startsWith(popup) ||
          typeof msg.tabId !== "number") return false;
      if (msg.type === "odm-tab-media") {
        tabMedia(msg.tabId).then(sendResponse);
        return true;
      }
      if (msg.type === "odm-grab" && msg.item) {
        chrome.tabs.get(msg.tabId).catch(() => null)
          .then((t) => grab(msg.item, popupReferrer(msg.item, t), (t && t.title) || ""))
          .then(sendResponse);
        return true;
      }
      return false;
    }
    if (msg.type === "odm-media-request") {
      tabMedia(tab.id).then((list) => sendResponse(list.filter((x) => x.frameId === sender.frameId)));
      return true; // async response
    }
    if (msg.type === "odm-grab" && msg.item) {
      grab(msg.item, sender.url || tab.url || "", tab.title || "").then(sendResponse);
      return true;
    }
    if (msg.type === "odm-bypass" && msg.url) {
      chrome.storage.session.get("bypass").then(({ bypass = {} }) => {
        bypass[msg.url] = Date.now();
        chrome.storage.session.set({ bypass });
      });
    }
    return false;
  });
}

if (typeof module !== "undefined") {
  module.exports = { hasType, configFresh, getConfig, hostMatches, isExcluded, mediaKind, streamFormat, normalizeMediaURL, totalSize, mergeMedia, popupReferrer, describePlaylist, sameDocument, DEFAULT_TYPES, CONFIG_TTL_MS, CONFIG_RETRY_MS };
}
