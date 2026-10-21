"""Rin - music bot for Echoed (echoed.gg). Joins a voice channel and plays songs you ask for."""
import asyncio, json, logging, os, random, re, shutil, sys, time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from array import array
from threading import Thread

import aiohttp
import socketio
import yt_dlp
from dotenv import load_dotenv
from flask import Flask
from livekit import rtc

load_dotenv()
TOKEN = os.environ["ECHOED_BOT_TOKEN"]
API = "https://go.echoed.gg/v1/bots"
SOCKET = "https://socket.echoed.gg"
PREFIX = "!"
COLOR = 0xFFC928
RATE, CHANNELS, SAMPLES = 48000, 2, 960            # 20 ms frames, as Echoed requires
FRAME_BYTES = SAMPLES * CHANNELS * 2               # 16-bit PCM
VOICE_FILE = "rin_voice.json"
SECRET_COOKIES = "/etc/secrets/youtube_cookies.txt"  # where Render mounts secret files
COOKIES = "youtube_cookies.txt"  # optional: browser cookies, helps when YouTube blocks downloads
if os.path.exists(SECRET_COOKIES):
    # Render's secret files are read-only, but yt-dlp wants to update its cookie file, so use a copy
    try:
        shutil.copyfile(SECRET_COOKIES, COOKIES)
    except OSError as e:
        logging.warning("couldn't copy the cookies file: %s", e)
BOT_ID = ""

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rin")


# ---------------------------------------------------------------- keep-alive web server
# Hosts like Render want a web server answering on $PORT, and an uptime pinger can hit it.
app = Flask("rin")
logging.getLogger("werkzeug").setLevel(logging.WARNING)  # don't log every ping


@app.route("/")
def home():
    return "Bot is running 24/7!"


def run_flask():
    port = int(os.environ.get("PORT", 8080))  # Render sets PORT automatically
    app.run(host="0.0.0.0", port=port)


def keep_alive():
    Thread(target=run_flask, daemon=True).start()


# ---------------------------------------------------------------- Echoed API
class Api:
    def __init__(self):
        self.s = None

    async def req(self, method, path, **kw):
        if self.s is None:
            self.s = aiohttp.ClientSession(headers={"X-Bot-Token": TOKEN})
        for _ in range(3):
            try:
                async with self.s.request(method, API + path, **kw) as r:
                    data = await r.json(content_type=None) if r.content_length != 0 else {}
                    if r.status == 429:
                        await asyncio.sleep(float((data or {}).get("retryAfter", 5)))
                        continue
                    if r.status >= 400:
                        log.warning("%s %s -> %s %s", method, path, r.status, data)
                        return None
                    return data if data is not None else {}
            except (aiohttp.ClientError, ValueError) as e:
                log.warning("%s %s failed: %s", method, path, e)
                await asyncio.sleep(2)


api = Api()


def card(title, desc):
    return {"type": "rich", "title": title, "description": desc, "color": COLOR, "footer": {"text": "Rin ♪"},
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}


async def send(server, channel, text="", embed=None):
    body = {"channelId": channel, "content": text, "mentions": [], "attachmentIds": []}
    if embed:
        body["embeds"] = [embed]
    return await api.req("POST", f"/{server}/messages/send", json=body)


def parse_time(text):
    """'1:50', '1:02:03', '90', '1m30s' -> seconds (or None)."""
    text = text.strip().lower().replace(" ", "")
    if re.fullmatch(r"\d+(:\d{1,2}){1,2}", text):
        secs = 0
        for part in text.split(":"):
            secs = secs * 60 + int(part)
        return secs
    if text.isdigit():
        return int(text)
    m = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", text)
    if m and any(m.groups()):
        h, mi, sec = (int(g or 0) for g in m.groups())
        return h * 3600 + mi * 60 + sec
    return None


def scale(data, percent):
    """Change the loudness of 16-bit PCM audio. 100 = unchanged."""
    if percent == 100:
        return data
    k = percent / 100
    return array("h", (max(-32768, min(32767, int(x * k))) for x in array("h", data))).tobytes()


def fmt_dur(s):
    if s is None:
        return "live"
    h, rem = divmod(int(s), 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


# ---------------------------------------------------------------- song lookup
@dataclass
class Track:
    title: str
    page: str
    stream: str
    headers: dict
    duration: float | None
    who: str = ""
    resolved: float = field(default_factory=time.time)


def _extract(query):
    opts = {"format": "bestaudio/best", "noplaylist": True, "quiet": True,  # warnings stay visible in the logs
            "check_formats": False,  # don't test-download formats before picking one
            "socket_timeout": 10}
    if os.path.exists(COOKIES):  # only use it if the file is actually there
        opts["cookiefile"] = COOKIES
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(query if re.match(r"https?://", query) else f"ytsearch1:{query}", download=False)
        if info and "entries" in info:
            info = next((e for e in info["entries"] if e), None)
        return info


def downloader_cmd(t):
    """Command that makes yt-dlp stream the song's audio to stdout (used if ffmpeg can't open the link itself)."""
    cmd = [sys.executable, "-m", "yt_dlp", "-f", "bestaudio/best", "--no-playlist", "-q", "-o", "-"]
    if os.path.exists(COOKIES):
        cmd += ["--cookies", COOKIES]
    return cmd + [t.page]


lookup_cache = {}  # query -> (time, Track), so replaying a song is instant


async def resolve(query):
    key = query.strip().lower()
    hit = lookup_cache.get(key)
    if hit and time.time() - hit[0] < 1800:
        return replace(hit[1])
    began = time.monotonic()
    try:
        info = await asyncio.get_running_loop().run_in_executor(None, _extract, query)
    except Exception as e:
        log.warning("yt-dlp failed for %r: %s", query, e)
        return None
    if not info or not info.get("url"):
        return None
    log.info("found %r in %.1fs (format %s, %s, %s)", info.get("title", "?"), time.monotonic() - began,
             info.get("format_id"), info.get("ext"), info.get("protocol"))
    track = Track(info.get("title", "Unknown"), info.get("webpage_url") or query, info["url"],
                  info.get("http_headers") or {}, info.get("duration"))
    lookup_cache[key] = (time.time(), track)
    return replace(track)


# ---------------------------------------------------------------- voice player
players = {}


class Player:
    def __init__(self, server, voice_id, text_channel):
        self.server, self.voice_id, self.text = server, voice_id, text_channel
        self.room, self.source, self.task, self.proc, self.play_task = None, None, None, None, None
        self.dl, self.pump = None, None  # yt-dlp process and the task feeding it to ffmpeg (fallback mode)
        self.q, self.current = [], None
        self.wake, self.playing = asyncio.Event(), asyncio.Event()
        self.playing.set()
        self.closed, self.stop_flag = False, False
        self.volume = saved_volume.get(server, 100)
        self.seek_to, self.offset, self.frames = None, 0, 0

    async def connect(self):
        d = await api.req("POST", f"/{self.server}/voice/{self.voice_id}/join")
        if not d or not d.get("token"):
            return False
        self.room = rtc.Room()
        self.room.on("disconnected", lambda *_: asyncio.create_task(self.close()))
        await self.room.connect(d["url"], d["token"])
        self.source = rtc.AudioSource(RATE, CHANNELS, queue_size_ms=300)  # small buffer = quick skip/pause
        track = rtc.LocalAudioTrack.create_audio_track("rin-music", self.source)
        pub = await self.room.local_participant.publish_track(
            track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE))
        log.info("published audio track (muted=%s)", getattr(pub, "muted", "unknown"))
        try:
            track.unmute()
        except Exception as e:
            log.info("track.unmute not available: %s", e)
        self.task = asyncio.create_task(self.run())
        return True

    @property
    def position(self):
        """Seconds into the current song."""
        return self.offset + self.frames * 0.02

    def skip_current(self):
        """Ask the playing song to stop at the next frame; run() then moves on to the next one."""
        if self.current is None:
            return
        self.stop_flag = True
        self.playing.set()  # wake it up if paused, and don't start the next song paused
        task = self.play_task
        # safety net: if it hasn't stopped by itself after 3 s, cancel it
        asyncio.get_running_loop().call_later(
            3, lambda: task.cancel() if task and not task.done() else None)

    @staticmethod
    async def _stop(p):
        """Kill a subprocess, reading its output pipe dry first (otherwise waiting on it can hang)."""
        if p is None:
            return
        if p.returncode is None:
            try:
                p.kill()
            except ProcessLookupError:
                pass
        try:
            async def drain():
                while await p.stdout.read(65536):
                    pass
                await p.wait()
            await asyncio.wait_for(drain(), 3)
        except (asyncio.TimeoutError, Exception) as e:
            log.warning("process didn't shut down cleanly: %r", e)

    async def kill_ffmpeg(self):
        p, dl, pump = self.proc, self.dl, self.pump
        self.proc = self.dl = self.pump = None
        if pump:
            pump.cancel()
        await self._stop(dl)
        await self._stop(p)

    async def close(self):
        if self.closed:
            return
        self.closed = True
        players.pop(self.server, None)
        self.q.clear()
        self.stop_flag = True
        self.playing.set()
        if self.play_task and not self.play_task.done():
            self.play_task.cancel()
        if self.task and self.task is not asyncio.current_task():
            self.task.cancel()
        await self.kill_ffmpeg()
        try:
            await self.room.disconnect()
        except Exception:
            pass
        await api.req("POST", f"/{self.server}/voice/{self.voice_id}/leave")

    async def run(self):
        while True:
            while not self.q:
                self.current = None
                self.wake.clear()
                await self.wake.wait()
            t = self.q.pop(0)
            self.current = t
            try:
                if time.time() - t.resolved > 600:  # stream URLs expire, so refresh old ones
                    fresh = await resolve(t.page)
                    if fresh:
                        t.stream, t.headers, t.resolved = fresh.stream, fresh.headers, fresh.resolved
                self.play_task = asyncio.create_task(self.stream(t))
                asyncio.create_task(send(self.server, self.text, embed=card(
                    "Now playing 🎶", f"**{t.title}**\n{fmt_dur(t.duration)} · requested by {t.who}")))
                await self.play_task
            except asyncio.CancelledError:
                if self.closed:
                    raise            # Rin is leaving
                # otherwise: this song was skipped/stopped, carry on to the next
            except Exception:
                log.exception("playback failed")
                await send(self.server, self.text, f"Couldn't play **{t.title}**, skipping.")
            finally:
                self.current, self.stop_flag, self.seek_to = None, False, None

    async def start_ffmpeg(self, t, offset, piped=False):
        """piped=False: ffmpeg opens the link itself. piped=True: yt-dlp downloads and feeds ffmpeg."""
        PIPE = asyncio.subprocess.PIPE
        cmd = ["ffmpeg", "-loglevel", "error"]
        if not piped:
            cmd += ["-nostdin"]
            if t.headers:
                cmd += ["-headers", "".join(f"{k}: {v}\r\n" for k, v in t.headers.items())]
        if offset:
            cmd += ["-ss", str(offset)]             # start from this many seconds in
        cmd += ["-i", "pipe:0" if piped else t.stream, "-vn", "-f", "s16le",
                "-ar", str(RATE), "-ac", str(CHANNELS), "pipe:1"]
        self.proc = await asyncio.create_subprocess_exec(
            *cmd, stdin=PIPE if piped else None, stdout=PIPE, stderr=PIPE)
        asyncio.create_task(self._log(self.proc, "ffmpeg"))
        if piped:
            self.dl = await asyncio.create_subprocess_exec(*downloader_cmd(t), stdout=PIPE, stderr=PIPE)
            asyncio.create_task(self._log(self.dl, "yt-dlp"))
            self.pump = asyncio.create_task(self._pump(self.dl, self.proc))

    async def stream(self, t):
        self.offset, self.frames, self.seek_to = 0, 0, None
        total, finished, began, piped = 0, False, time.monotonic(), False
        log.info("streaming: %s", t.title)
        try:
            while True:
                await self.start_ffmpeg(t, self.offset, piped)
                start, n, self.frames, finished = time.monotonic(), 0, 0, False
                while not self.stop_flag and self.seek_to is None:
                    if not self.playing.is_set():       # paused
                        await self.playing.wait()
                        start, n = time.monotonic(), 0
                        continue
                    try:
                        data = await self.proc.stdout.readexactly(FRAME_BYTES)
                    except asyncio.IncompleteReadError:
                        finished = True
                        break                           # song ended (or ffmpeg gave nothing)
                    data = scale(data, self.volume)
                    await self.source.capture_frame(rtc.AudioFrame(data, RATE, CHANNELS, SAMPLES))
                    n += 1
                    total += 1
                    self.frames += 1
                    if total == 1:
                        log.info("first audio sent %.1fs after starting%s", time.monotonic() - began,
                                 " (via yt-dlp)" if piped else "")
                    ahead = start + n * 0.02 - time.monotonic()
                    if ahead > 0.15:                    # don't run far ahead of real time
                        await asyncio.sleep(ahead - 0.15)
                if self.seek_to is not None and not self.stop_flag:
                    self.offset, self.seek_to = self.seek_to, None
                    await self.kill_ffmpeg()
                    continue                            # restart ffmpeg from the new spot
                if finished and self.frames == 0 and not piped and not self.stop_flag:
                    code = None
                    try:
                        await asyncio.wait_for(self.proc.wait(), 2)
                        code = self.proc.returncode
                    except Exception:
                        pass
                    log.warning("ffmpeg got no audio from the direct link (exit code %s); retrying through yt-dlp", code)
                    for k in [k for k, (_, tr) in lookup_cache.items() if tr.page == t.page]:
                        del lookup_cache[k]             # don't reuse a link that didn't work
                    await self.kill_ffmpeg()
                    piped = True
                    continue
                break
        finally:
            await self.kill_ffmpeg()
            log.info("sent %d audio frames (~%.0f s) for %s", total, total * 0.02, t.title)
            if finished and total == 0:
                await send(self.server, self.text, "⚠️ I couldn't get any audio from that source. Check the logs for yt-dlp or ffmpeg errors.")

    @staticmethod
    async def _pump(dl, ff):
        """Copy yt-dlp's output into ffmpeg's input."""
        try:
            while chunk := await dl.stdout.read(65536):
                ff.stdin.write(chunk)
                await ff.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            try:
                ff.stdin.close()
            except Exception:
                pass

    @staticmethod
    async def _log(proc, name):
        async for line in proc.stderr:
            log.warning("%s: %s", name, line.decode(errors="replace").strip())


# ---------------------------------------------------------------- commands
def load_voice():
    try:
        with open(VOICE_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


saved_voice = load_voice()  # server_id -> last voice channel id
VOLUME_FILE = "rin_volume.json"
try:
    with open(VOLUME_FILE) as _f:
        saved_volume = json.load(_f)  # server_id -> volume percent
except (OSError, ValueError):
    saved_volume = {}


async def find_channel(server, ref):
    d = await api.req("GET", f"/{server}/channels")
    chans = d.get("channels", []) if isinstance(d, dict) else (d or [])
    for c in chans:
        if ref.lower().lstrip("#") in (str(c.get("name", "")).lower(), str(c.get("id", "")).lower()):
            return c["id"]


class Ctx:
    def __init__(self, msg, args):
        self.msg, self.args = msg, args
        self.uid = str(msg.get("senderId"))
        self.server, self.channel = msg["serverId"], msg["channelId"]
        self.name = (msg.get("author") or {}).get("name", "someone")

    async def say(self, text="", embed=None):
        return await send(self.server, self.channel, text, embed)


async def ensure_player(ctx, voice_ref=None):
    """Return this server's player, joining a voice channel first if needed."""
    p = players.get(ctx.server)
    if voice_ref:
        vid = await find_channel(ctx.server, voice_ref)
        if not vid:
            await ctx.say(f"I can't find a channel called **{voice_ref}**.")
            return None
        if p and p.voice_id != vid:
            await p.close()
            p = None
        saved_voice[ctx.server] = vid
        with open(VOICE_FILE, "w") as f:
            json.dump(saved_voice, f)
    if p:
        p.text = ctx.channel
        return p
    vid = saved_voice.get(ctx.server)
    if not vid:
        await ctx.say("Which voice channel? Use `!join <voice channel name>` once, then `!play` works on its own.")
        return None
    perms = await api.req("GET", f"/{ctx.server}/members/{BOT_ID}/permissions", params={"channel_id": vid})
    have = set((perms or {}).get("permissions", []))
    log.info("my permissions in that voice channel: %s", sorted(have))
    if perms and "ADMINISTRATOR" not in have and not {"CONNECT", "SPEAK"} <= have:
        await ctx.say("⚠️ I'm missing **Connect** and/or **Speak** in that voice channel. "
                      "Give my role those permissions (check the channel's own permission overrides too).")
    p = Player(ctx.server, vid, ctx.channel)
    try:
        ok = await p.connect()
    except Exception:
        log.exception("voice connect failed")
        ok = False
    if not ok:
        await ctx.say("I couldn't join that channel. Make sure my role has **Connect** and **Speak** there.")
        return None
    players[ctx.server] = p
    return p


def need_player(fn):
    async def wrapper(ctx):
        p = players.get(ctx.server)
        if not p:
            return await ctx.say("I'm not in a voice channel.")
        await fn(ctx, p)
    return wrapper


COMMANDS = {}


def command(*names):
    def deco(f):
        for n in names:
            COMMANDS[n] = f
        return f
    return deco


@command("help")
async def c_help(ctx):
    await ctx.say(embed=card("Hi, I'm Rin! 🎀", "\n".join([
        "`!join <voice channel>`: join a voice channel (only needed once)",
        "`!play <song or link>`: play a song, or add it to the queue",
        "`!queue`: show the queue · `!nowplaying`: current song · `!remove <number>`",
        "`!pause` · `!resume` · `!skip` · `!time 1:50` · `!volume <0-150>`",
        "`!stop`: stop the song and clear the queue (I stay in the channel)",
        "`!leave`: stop everything and leave",
        "`!spin`: pull a random vocaloid / UTAUloid / fanloid · `!collection` · `!odds`",
    ])))


@command("join")
async def c_join(ctx):
    p = await ensure_player(ctx, " ".join(ctx.args) or None)
    if p:
        await ctx.say("🎤 I'm in! Use `!play <song>`.")


@command("play", "p")
async def c_play(ctx):
    if not ctx.args:
        return await ctx.say("Usage: `!play <song name or link>`")
    lookup = asyncio.create_task(resolve(" ".join(ctx.args)))  # search while joining the channel
    await ctx.say("🔎 Looking for it...")
    p = await ensure_player(ctx)
    if not p:
        lookup.cancel()
        return
    track = await lookup
    if not track:
        return await ctx.say("I couldn't find that song.")
    track.who = ctx.name
    busy = bool(p.q) or bool(p.play_task and not p.play_task.done())  # something playing or waiting?
    p.q.append(track)
    p.wake.set()
    if busy:
        await ctx.say(f"➕ Queued **{track.title}** (position {len(p.q)})")


@command("pause")
@need_player
async def c_pause(ctx, p):
    if not p.current:
        return await ctx.say("Nothing is playing.")
    p.playing.clear()
    await ctx.say("⏸️ Paused.")


@command("resume", "unpause")
@need_player
async def c_resume(ctx, p):
    p.playing.set()
    await ctx.say("▶️ Resumed.")


@command("skip", "next")
@need_player
async def c_skip(ctx, p):
    if not p.current:
        return await ctx.say("Nothing is playing.")
    p.skip_current()
    await ctx.say("⏭️ Skipped.")


@command("stop")
@need_player
async def c_stop(ctx, p):
    p.q.clear()
    p.skip_current()
    await ctx.say("⏹️ Stopped and cleared the queue.")


@command("leave", "disconnect", "dc")
@need_player
async def c_leave(ctx, p):
    await p.close()
    await ctx.say("👋 Bye bye!")


@command("time", "seek", "jump")
@need_player
async def c_time(ctx, p):
    t = p.current
    if not t:
        return await ctx.say("Nothing is playing.")
    if not ctx.args:
        return await ctx.say(f"⏱️ {fmt_dur(p.position)} / {fmt_dur(t.duration)}")
    secs = parse_time(" ".join(ctx.args))
    if secs is None:
        return await ctx.say("Try `!time 1:50`, `!time 90` or `!time 1m30s`.")
    if t.duration is None:
        return await ctx.say("I can't jump around in a live stream.")
    if secs >= t.duration:
        return await ctx.say(f"That song is only {fmt_dur(t.duration)} long.")
    p.seek_to = secs
    p.playing.set()  # jumping also resumes if paused
    await ctx.say(f"⏩ Jumped to **{fmt_dur(secs)}**.")


@command("volume", "vol")
async def c_volume(ctx):
    p = players.get(ctx.server)
    current = p.volume if p else saved_volume.get(ctx.server, 100)
    if not ctx.args:
        return await ctx.say(f"🔊 Volume is **{current}%**. Change it with `!volume <0-150>`.")
    m = re.fullmatch(r"(\d{1,3})%?", ctx.args[0])
    if not m or int(m[1]) > 150:
        return await ctx.say("Pick a number from 0 to 150, like `!volume 50`.")
    v = int(m[1])
    saved_volume[ctx.server] = v
    with open(VOLUME_FILE, "w") as f:
        json.dump(saved_volume, f)
    if p:
        p.volume = v
    icon = "🔇" if v == 0 else "🔉" if v < 50 else "🔊"
    await ctx.say(f"{icon} Volume set to **{v}%**." + (" (Above 100% can sound distorted.)" if v > 100 else ""))


@command("queue", "q")
@need_player
async def c_queue(ctx, p):
    lines = [f"**Now:** {p.current.title}"] if p.current else []
    lines += [f"`{i}.` {t.title} ({fmt_dur(t.duration)})" for i, t in enumerate(p.q[:10], 1)]
    if len(p.q) > 10:
        lines.append(f"...and {len(p.q) - 10} more")
    await ctx.say(embed=card("Queue", "\n".join(lines) or "Nothing queued."))


@command("nowplaying", "np")
@need_player
async def c_np(ctx, p):
    t = p.current
    if not t:
        return await ctx.say("Nothing is playing.")
    await ctx.say(embed=card("Now playing 🎶", f"**{t.title}**\n{fmt_dur(p.position)} / {fmt_dur(t.duration)} · requested by {t.who}"))


@command("remove")
@need_player
async def c_remove(ctx, p):
    if ctx.args and ctx.args[0].isdigit() and 1 <= int(ctx.args[0]) <= len(p.q):
        t = p.q.pop(int(ctx.args[0]) - 1)
        return await ctx.say(f"Removed **{t.title}**.")
    await ctx.say("Usage: `!remove <queue number>`")


# ---------------------------------------------------------------- spin (gacha)
CHARACTERS = [  # (name, type, weight): a bigger weight means a more common pull
    ("Hatsune Miku", "Vocaloid", 120), ("Kagamine Rin", "Vocaloid", 100), ("Kagamine Len", "Vocaloid", 100),
    ("Megurine Luka", "Vocaloid", 90), ("KAITO", "Vocaloid", 80), ("MEIKO", "Vocaloid", 80),
    ("GUMI", "Vocaloid", 70), ("IA", "Vocaloid", 40), ("SeeU", "Vocaloid", 20),
    ("Yuzuki Yukari", "Vocaloid", 20), ("Kamui Gakupo", "Vocaloid", 20), ("Oliver", "Vocaloid", 8),
    ("Lily", "Vocaloid", 6), ("Hiyama Kiyoteru", "Vocaloid", 4), ("Nekomura Iroha", "Vocaloid", 4),
    ("Mayu", "Vocaloid", 3), ("Fukase", "Vocaloid", 1),
    ("Kasane Teto", "UTAUloid", 70), ("Defoko", "UTAUloid", 40), ("Momone Momo", "UTAUloid", 30),
    ("Namine Ritsu", "UTAUloid", 30),
    ("Yowane Haku", "Fanloid", 60), ("Akita Neru", "Fanloid", 60), ("Sukone Tei", "Fanloid", 15),
    ("Hatsune Mikuo", "Fanloid", 12), ("Honne Dell", "Fanloid", 10), ("Zatsune Miku", "Fanloid", 8),
]
TOTAL = sum(c[2] for c in CHARACTERS)
INFO = {n: (k, w) for n, k, w in CHARACTERS}
COLLECTION_FILE = "rin_collection.json"
spin_cooldown = {}


def odds_of(w):
    return round(TOTAL / w)


def tier(odds):
    for limit, label in ((15, "Common ⚪"), (60, "Uncommon 🟢"), (200, "Rare 🔵"), (600, "Epic 🟣")):
        if odds <= limit:
            return label
    return "Legendary 🌟"


def load_collection():
    try:
        with open(COLLECTION_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


collection = load_collection()  # server_id -> user_id -> {character: count}


@command("spin", "roll")
async def c_spin(ctx):
    now = time.time()
    if now - spin_cooldown.get(ctx.uid, 0) < 3:
        return await ctx.say("Slow down, the wheel is still spinning! 🎡")
    spin_cooldown[ctx.uid] = now
    name, kind, w = random.choices(CHARACTERS, weights=[c[2] for c in CHARACTERS])[0]
    mine = collection.setdefault(ctx.server, {}).setdefault(ctx.uid, {})
    mine[name] = mine.get(name, 0) + 1
    with open(COLLECTION_FILE, "w") as f:
        json.dump(collection, f)
    o = odds_of(w)
    status = "✨ **NEW!**" if mine[name] == 1 else f"(you have {mine[name]})"
    if name == "Kagamine Rin":
        status += "\nThat's me! 💛"
    await ctx.say(embed=card(f"🎲 {ctx.name} pulled {name}!",
                             f"{kind} · {tier(o)}\nOdds: **1 in {o}** ({w / TOTAL * 100:.2f}%)\n{status}"))


@command("collection", "col", "inv")
async def c_collection(ctx):
    mine = collection.get(ctx.server, {}).get(ctx.uid, {})
    if not mine:
        return await ctx.say("You haven't pulled anyone yet. Try `!spin`!")
    rows = sorted(mine.items(), key=lambda kv: INFO[kv[0]][1])  # rarest first
    lines = [f"**{n}** x{c}: 1 in {odds_of(INFO[n][1])}" for n, c in rows if n in INFO]
    await ctx.say(embed=card(f"{ctx.name}'s collection ({len(lines)}/{len(CHARACTERS)})", "\n".join(lines)))


@command("odds")
async def c_odds(ctx):
    lines = []
    for kind in ("Vocaloid", "UTAUloid", "Fanloid"):
        lines.append(f"**{kind}s**")
        for n, k, w in sorted(CHARACTERS, key=lambda c: -c[2]):
            if k == kind:
                lines.append(f"1 in {odds_of(w)}: {n}")
    await ctx.say(embed=card("Spin odds 🎰", "\n".join(lines)))


# ---------------------------------------------------------------- events
sio = socketio.AsyncClient(reconnection=True)


@sio.event
async def connect():
    await sio.emit("authenticate", {"botToken": TOKEN})


@sio.on("authenticated")
async def on_auth(payload):
    log.info("Rin is online!")


@sio.on("MESSAGE_CREATE")
async def on_message(msg):
    try:
        author = msg.get("author") or {}
        if author.get("isBot") or str(msg.get("senderId")) == BOT_ID or not msg.get("serverId"):
            return
        text = (msg.get("content") or "").strip()
        if not text.startswith(PREFIX):
            return
        parts = text[len(PREFIX):].split()
        if parts and (fn := COMMANDS.get(parts[0].lower())):
            log.info("command !%s from %s", parts[0].lower(), author.get("name", "?"))
            await fn(Ctx(msg, parts[1:]))
    except Exception:
        log.exception("command failed")


async def heartbeat_loop():
    while True:
        await asyncio.sleep(30)
        if sio.connected:
            await sio.emit("heartbeat", {"botToken": TOKEN})


async def main():
    global BOT_ID
    if not shutil.which("ffmpeg"):
        raise SystemExit("ffmpeg isn't installed (or not on PATH). Install it, reopen the terminal, and try again.")
    try:
        v = await api.req("GET", "/validate")
        if not v or not v.get("valid"):
            raise SystemExit("Echoed rejected the token. See the warning above for why.")
        BOT_ID = str(v["bot_id"])
        asyncio.create_task(heartbeat_loop())
        await sio.connect(SOCKET, transports=["websocket"])
        await sio.wait()
    finally:
        for p in list(players.values()):
            await p.close()
        if api.s:
            await api.s.close()


if __name__ == "__main__":
    keep_alive()  # start the web server thread, then run the bot
    asyncio.run(main())
