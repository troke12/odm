// Run with: node --test extension/test/*.test.js
const test = require("node:test");
const assert = require("node:assert/strict");
const bg = require("../background.js");

test("mediaKind", () => {
  assert.equal(bg.mediaKind("https://cdn/media/play?expire=1", "video/mp4"), "file");
  assert.equal(bg.mediaKind("https://cdn/x/master.m3u8", ""), "stream");
  assert.equal(bg.mediaKind("https://cdn/x/manifest", "application/dash+xml"), "stream");
  assert.equal(bg.mediaKind("https://cdn/x/seg-001.ts", "video/mp2t"), null); // HLS segment
  assert.equal(bg.mediaKind("https://cdn/x/chunk-5.m4s", "video/mp4"), null); // fMP4 segment
  assert.equal(bg.mediaKind("https://cdn/x/seg", "audio/iso.segment"), null);
  assert.equal(bg.mediaKind("https://cdn/clip.mp4", "application/octet-stream"), "file");
  assert.equal(bg.mediaKind("https://cdn/page.html", "text/html"), null);
  assert.equal(bg.mediaKind("https://cdn/song", "audio/mpeg; charset=x"), "file");
});

test("normalizeMediaURL drops the fragment and keeps the query", () => {
  assert.equal(bg.normalizeMediaURL("https://cdn.example/v.mp4?range=5&rn=3#t=1"), "https://cdn.example/v.mp4?range=5&rn=3");
  assert.equal(bg.normalizeMediaURL("not a url"), "not a url");
});

test("totalSize", () => {
  assert.equal(bg.totalSize([{ name: "Content-Range", value: "bytes 0-1048575/69652480" }]), 69652480);
  assert.equal(bg.totalSize([{ name: "content-length", value: "1234" }]), 1234);
  assert.equal(bg.totalSize([]), 0);
  assert.equal(bg.totalSize([{ name: "Content-Range", value: "bytes 0-9/*" }, { name: "Content-Length", value: "10" }]), 10);
});

test("exclusions", () => {
  assert.ok(bg.hostMatches("download.windowsupdate.com", "*.windowsupdate.com"));
  assert.ok(bg.hostMatches("windowsupdate.com", "*.windowsupdate.com"));
  assert.ok(!bg.hostMatches("evilwindowsupdate.com", "*.windowsupdate.com"));
  assert.ok(bg.isExcluded("https://Example.org/a.zip", ["example.org"]));
  assert.ok(!bg.isExcluded("https://sub.example.org/a.zip", ["example.org"]));
});

test("hasType checks the URL and the browser's file name", () => {
  assert.ok(bg.hasType("https://h/a.ZIP?x=1", ["zip"]));
  assert.ok(bg.hasType("https://h/download?id=3", [".exe"], "C:\\Users\\u\\Downloads\\setup.exe"));
  assert.ok(!bg.hasType("https://h/page", ["zip"], ""));
});

test("mergeMedia dedupes, keeps the larger size, caps the list", () => {
  let list = bg.mergeMedia([], { url: "a", size: 100 });
  list = bg.mergeMedia(list, { url: "a", size: 0 });
  assert.equal(list.length, 1);
  assert.equal(list[0].size, 100);
  for (let i = 0; i < 40; i++) list = bg.mergeMedia(list, { url: "u" + i, size: i });
  assert.equal(list.length, 15);
  assert.equal(list[0].url, "u39");
});

test("mergeMedia keeps when an item was first seen", () => {
  let list = bg.mergeMedia([], { url: "m", size: 0, at: 5 });
  list = bg.mergeMedia(list, { url: "m", size: 0, at: 9 });
  assert.equal(list[0].at, 5);
  assert.equal(bg.mergeMedia([], { url: "x", size: 0 })[0].at, undefined);
});

test("mergeMedia drops files before playlists when full", () => {
  let list = bg.mergeMedia([], { url: "master", kind: "stream", size: 0 });
  for (let i = 0; i < 40; i++) list = bg.mergeMedia(list, { url: "f" + i, kind: "file", size: i });
  assert.equal(list.length, 15);
  assert.ok(list.some((x) => x.url === "master"));
  assert.equal(list[0].url, "f39");
});

test("streamFormat", () => {
  assert.equal(bg.streamFormat("https://cdn/x/master.m3u8", ""), "HLS");
  assert.equal(bg.streamFormat("https://cdn/x/play", "application/x-mpegURL; charset=utf-8"), "HLS");
  assert.equal(bg.streamFormat("https://cdn/x/manifest", "application/dash+xml"), "DASH");
  assert.equal(bg.streamFormat("https://cdn/x/m.mpd?t=1", "text/plain"), "DASH");
  assert.equal(bg.streamFormat("https://cdn/x/video", "video/mp4"), "");
});

test("popupReferrer", () => {
  const tab = { url: "https://site.example/watch?v=1" };
  assert.equal(bg.popupReferrer({ frameId: 0, origin: "https://site.example" }, tab), tab.url);
  assert.equal(bg.popupReferrer({ frameId: 5, page: "https://player.example/embed/9" }, tab), "https://player.example/embed/9");
  assert.equal(bg.popupReferrer({ frameId: 5, origin: "https://player.example" }, tab), "https://player.example/");
  assert.equal(bg.popupReferrer({ frameId: 5 }, tab), tab.url);
  assert.equal(bg.popupReferrer({ frameId: 5 }, null), "");
});

test("describePlaylist", () => {
  const master = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=6000000,RESOLUTION=1920x1080\nchunked.m3u8\n" +
    "#EXT-X-STREAM-INF:BANDWIDTH=1400000,RESOLUTION=852x480\n480p.m3u8\n";
  assert.deepEqual(bg.describePlaylist(master, "HLS", false), { master: true, quality: "All qualities, up to 1080p", live: false });
  const live = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\na.ts\n";
  assert.deepEqual(bg.describePlaylist(live, "HLS", false), { master: false, quality: "One quality", live: true });
  assert.deepEqual(bg.describePlaylist(live, "HLS", true), { master: false, quality: "One quality", live: false });
  assert.equal(bg.describePlaylist(live + "#EXT-X-ENDLIST\n", "HLS", false).live, false);
  // One variant with separate audio is still the playlist to pick.
  const one = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"a\",URI=\"a.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=1,RESOLUTION=1280x720,AUDIO=\"a\"\nv.m3u8\n";
  assert.deepEqual(bg.describePlaylist(one, "HLS", false), { master: true, quality: "One quality, 720p", live: false });
  const mpd = "<?xml version='1.0'?><mpd:MPD xmlns:mpd='urn:mpeg:dash:schema:mpd:2011' type='dynamic'><Period>" +
    '<AdaptationSet height="720"><Representation id="a"/></AdaptationSet><AdaptationSet><Representation id="b" height="1080"/></AdaptationSet></Period></mpd:MPD>';
  assert.deepEqual(bg.describePlaylist(mpd, "DASH", false), { master: true, quality: "All qualities, up to 1080p", live: true });
  assert.equal(bg.describePlaylist("<html>error</html>", "HLS", false), null);
  assert.equal(bg.describePlaylist("#EXTM3U", "DASH", false), null);
});

test("describePlaylist stays linear on hostile input", () => {
  // Compares 128 KB with 512 KB rather than against a clock: quadratic
  // work grows 16 times for 4 times the input, linear work about 4 times.
  const time = (unit, size) => {
    const text = (unit.startsWith("#") ? "#EXTM3U\n" : "") + unit.repeat(Math.ceil(size / unit.length));
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < 3; i++) bg.describePlaylist(text, unit.startsWith("#") ? "HLS" : "DASH", true);
    return Number(process.hrtime.bigint() - t0) / 3e6;
  };
  for (const unit of ["<Representation ", "<MPD ", "<mpd:MPD type=", "#EXT-X-STREAM-INF:RESOLUTION=1x"]) {
    const small = Math.max(time(unit, 128 * 1024), 1);
    const big = time(unit, 512 * 1024);
    assert.ok(big < small * 8 && big < 2000, `${unit.trim()}: ${small.toFixed(1)} ms for 128 KB, ${big.toFixed(1)} ms for 512 KB`);
  }
});

test("normalizeMediaURL drops low-latency HLS reload parameters", () => {
  assert.equal(bg.normalizeMediaURL("https://cdn.example/live.m3u8?token=a&_HLS_msn=120&_HLS_part=3&_HLS_skip=YES"),
    "https://cdn.example/live.m3u8?token=a");
  // Everything else stays byte for byte (signed queries).
  assert.equal(bg.normalizeMediaURL("https://cdn.example/l.m3u8?hdnts=exp=1~acl=/*~hmac=ab&q=a%20b&_HLS_msn=3"),
    "https://cdn.example/l.m3u8?hdnts=exp=1~acl=/*~hmac=ab&q=a%20b");
  assert.equal(bg.normalizeMediaURL("https://cdn.example/l.m3u8?_HLS_msn=3"), "https://cdn.example/l.m3u8");
});

test("mergeMedia keeps what a peek learned", () => {
  const first = { url: "u", kind: "stream", size: 0, at: 1, master: true, quality: "All qualities", live: true };
  const next = bg.mergeMedia([first], { url: "u", kind: "stream", size: 0, at: 5 });
  assert.equal(next[0].master, true);
  assert.equal(next[0].quality, "All qualities");
  assert.equal(next[0].live, true);
  assert.equal(next[0].at, 1);
});

test("sameDocument", () => {
  assert.equal(bg.sameDocument("https://s.example/watch?v=1#t=10", "https://s.example/watch?v=1#comments"), true);
  assert.equal(bg.sameDocument("https://s.example/watch?v=1", "https://s.example/watch?v=1#x"), true);
  assert.equal(bg.sameDocument("https://s.example/watch?v=1", "https://s.example/watch?v=2"), false);
  assert.equal(bg.sameDocument("https://s.example/a", "https://s.example/b"), false);
  assert.equal(bg.sameDocument("not a url", "not a url"), false);
});

test("configFresh keeps ODM's answer longer than the fallback", () => {
  const now = 1_000_000;
  assert.equal(bg.configFresh(undefined, now), false);
  assert.equal(bg.configFresh({ at: 0 }, now), false); // the old "retry next time" entry
  assert.equal(bg.configFresh({ ok: true, at: now - bg.CONFIG_TTL_MS + 1 }, now), true);
  assert.equal(bg.configFresh({ ok: true, at: now - bg.CONFIG_TTL_MS }, now), false);
  assert.equal(bg.configFresh({ ok: false, at: now - bg.CONFIG_RETRY_MS + 1 }, now), true);
  assert.equal(bg.configFresh({ ok: false, at: now - bg.CONFIG_RETRY_MS }, now), false);
});

test("getConfig with ODM down asks once, for concurrent calls and after", async () => {
  // A fake chrome with ODM unreachable: each sendNativeMessage is one host
  // process the browser would start.
  let asks = 0;
  const store = {};
  globalThis.chrome = {
    runtime: {
      lastError: undefined,
      sendNativeMessage(_host, _msg, cb) {
        asks++;
        setTimeout(() => {
          chrome.runtime.lastError = { message: "Specified native messaging host not found." };
          cb(undefined);
          chrome.runtime.lastError = undefined;
        }, 5);
      },
    },
    storage: { session: {
      async get(k) { return { [k]: store[k] }; },
      async set(o) { Object.assign(store, o); },
    } },
  };
  try {
    const got = await Promise.all(Array.from({ length: 20 }, () => bg.getConfig()));
    assert.equal(asks, 1);
    assert.deepEqual(got[0].fileTypes, bg.DEFAULT_TYPES);
    await bg.getConfig();
    assert.equal(asks, 1, "a failed fetch is cached for CONFIG_RETRY_MS");
  } finally {
    delete globalThis.chrome;
  }
});
