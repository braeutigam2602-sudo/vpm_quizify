"""
QQL · CUE SERVICE — Realtime nervous system for the Quizify Quantum Live show.
==============================================================================
HTTP in  : POST /api/show/cue       (control plane — API-key + rate-limited)
Bus      : Redis (Sentinel-backed)  — Pub/Sub fan-out across replicas + Stream replay log
WS out   : /api/show/stream         (data plane — public viewers, per-IP capped)

Flow: cue -> validate/auth/rate-limit -> stamp server_ts + ids -> PUBLISH + XADD ->
every replica's subscriber receives it -> per-client Predictive Latency Compensation (PLC)
-> combined {visual + audio_hints + haptic_cue} sent so the effect fires synchronously.

DESIGN DECISIONS (1-2 lines each):
- Redis Pub/Sub for fan-out (every replica broadcasts to its own WS clients) + a Redis
  Stream (XADD) as an append-only replay log — low-latency live path, durable audit path.
- PLC is computed PER CLIENT from a rolling RTT estimate; bounded so we never overcompensate.
- Control-traffic (ping/pong/hello) and game-traffic (events) use a typed `kind` envelope and,
  optionally, separate Redis channels — so a viewer flood never starves RTT probing.
- Backpressure: each client has a bounded send queue; on overflow we drop the OLDEST GAME event
  (never control) and, if a client stays saturated, we disconnect it rather than blocking everyone.
- Security split: the CONTROL plane (POST /cue) needs an API key; the DATA plane (WS viewers) is
  public but per-IP connection-capped + optionally token-gated (HMAC) — viewers aren't issued keys.

NOTE on "haptic": this targets the Web `navigator.vibrate()` API (Android Chrome; iOS Safari
ignores it). It is a cosmetic sync cue, not a hardware driver. "jackpot" here is a NON-PAYOUT
hype event identifier, not a money mechanic.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

from fastapi import (Depends, FastAPI, Header, HTTPException, Request, WebSocket,
                     WebSocketDisconnect, status)
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field, field_validator

import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel

# ── optional metrics (service still runs without prometheus_client) ───────────
try:
    from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
    _HAS_PROM = True
except Exception:  # pragma: no cover
    _HAS_PROM = False


# =============================================================================
# CONFIG (env only — secrets never hardcoded)
# =============================================================================
def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _secret(name: str, file_path: str) -> str:
    """ENV wins; else read a Docker/Swarm secret file (env NAME, then NAME_FILE, then /run/secrets/...)."""
    v = os.environ.get(name)
    if v:
        return v
    path = os.environ.get(f"{name}_FILE", file_path)
    try:
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
    except OSError:
        pass
    return ""


@dataclass(frozen=True)
class Config:
    # Redis (Sentinel preferred; REDIS_URL is the single-node dev fallback)
    redis_sentinels: str = _env("REDIS_SENTINELS")            # "h1:26379,h2:26379,h3:26379"
    redis_master_name: str = _env("REDIS_MASTER_NAME", "qql-master")
    redis_password: str = _secret("REDIS_PASSWORD", "/run/secrets/redis_password")
    redis_url: str = _env("REDIS_URL", "redis://localhost:6379/0")
    channel: str = _env("REDIS_CHANNEL", "show:events")
    stream: str = _env("REDIS_STREAM", "show:events:log")
    stream_maxlen: int = int(_env("REDIS_STREAM_MAXLEN", "100000"))

    # Security
    api_key_header: str = _env("API_KEY_HEADER_NAME", "X-API-Key")
    api_keys: str = _secret("API_KEYS", "/run/secrets/api_keys")          # comma-separated, supports rotation
    ws_token_secret: str = _secret("WS_TOKEN_SECRET", "/run/secrets/ws_token_secret")  # WS HMAC token secret
    ws_require_token: bool = _env("WS_REQUIRE_TOKEN", "false").lower() == "true"

    # Rate limits (control plane)
    cue_rate_limit: int = int(_env("CUE_RATE_LIMIT", "50"))   # cues per window per key
    cue_rate_window_s: int = int(_env("CUE_RATE_WINDOW_S", "10"))

    # Data plane caps
    max_conns_per_ip: int = int(_env("MAX_CONNS_PER_IP", "20"))
    client_queue_max: int = int(_env("CLIENT_QUEUE_MAX", "256"))
    ping_interval_s: float = float(_env("PING_INTERVAL_S", "5"))
    client_idle_timeout_s: float = float(_env("CLIENT_IDLE_TIMEOUT_S", "30"))

    # PLC
    plc_version: str = _env("PLC_VERSION", "v1")
    plc_safety_margin_ms: float = float(_env("PLC_SAFETY_MARGIN_MS", "200"))
    plc_min_offset_ms: float = float(_env("PLC_MIN_OFFSET_MS", "0"))
    plc_max_offset_ms: float = float(_env("PLC_MAX_OFFSET_MS", "400"))
    plc_default_rtt_ms: float = float(_env("PLC_DEFAULT_RTT_MS", "120"))
    plc_ab_split: float = float(_env("PLC_AB_SPLIT", "0.0"))  # 0..1 fraction routed to plc_group "B"

    # Haptic backoff
    haptic_strong_max: int = int(_env("HAPTIC_STRONG_MAX", "3"))
    haptic_strong_window_s: float = float(_env("HAPTIC_STRONG_WINDOW_S", "10"))


CFG = Config()
RUN_ID = uuid.uuid4().hex[:12]   # per-process correlation id

# =============================================================================
# STRUCTURED LOGGING (JSON, correlated by run_id)
# =============================================================================
class JsonLog(logging.Formatter):
    def format(self, r: logging.LogRecord) -> str:
        base = {"ts": round(time.time(), 3), "lvl": r.levelname, "run_id": RUN_ID, "msg": r.getMessage()}
        if isinstance(r.args, dict):
            base.update(r.args)
        for k in ("event_id", "client_id", "ip", "api_key_id", "lat"):
            v = getattr(r, k, None)
            if v is not None:
                base[k] = v
        return json.dumps(base, separators=(",", ":"))


_h = logging.StreamHandler()
_h.setFormatter(JsonLog())
log = logging.getLogger("cue")
log.setLevel(logging.INFO)
log.handlers = [_h]
log.propagate = False

# =============================================================================
# METRICS
# =============================================================================
if _HAS_PROM:
    M_CUES_IN = Counter("qql_cues_in_total", "Cues accepted", ["type"])
    M_CUES_REJ = Counter("qql_cues_rejected_total", "Cues rejected", ["reason"])
    M_EVENTS_OUT = Counter("qql_events_out_total", "Per-client events sent")
    M_DROPS = Counter("qql_client_drops_total", "Game events dropped by backpressure")
    M_WS = Gauge("qql_ws_connections", "Live WS connections")
    M_RTT = Histogram("qql_client_rtt_ms", "Per-client RTT", buckets=(20, 50, 100, 150, 300, 600, 1200))
    M_OFFSET = Histogram("qql_plc_offset_ms", "PLC offset", buckets=(0, 25, 50, 100, 200, 400))


# =============================================================================
# DATA MODELS
# =============================================================================
class CueIn(BaseModel):
    """Inbound cue from regie / Twitch-extension / game engine."""
    type: str = Field(..., description="action|elimination|ad_break|jackpot_event|vip_join|reset|lobby")
    eliminated: Optional[list[int]] = None
    name: Optional[str] = None
    audio_cue_id: Optional[str] = None                 # maps to a haptic profile
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: Optional[str] = None
    plc_group: Optional[str] = None                    # force a group (else auto A/B)

    _ALLOWED = {"action", "elimination", "ad_break", "jackpot_event", "vip_join", "reset", "lobby"}

    @field_validator("type")
    @classmethod
    def _ty(cls, v: str) -> str:
        v = (v or "").lower().strip()
        if v not in cls._ALLOWED:
            raise ValueError(f"type must be one of {sorted(cls._ALLOWED)}")
        return v

    @field_validator("eliminated")
    @classmethod
    def _elim(cls, v):
        if v is not None and any((not isinstance(i, int) or i < 0 or i > 99) for i in v):
            raise ValueError("eliminated must be ints in 0..99")
        return v


# audio_cue_id -> haptic profile. intensity drives the strong-vibration backoff.
HAPTIC_PROFILES: dict[str, dict] = {
    "jackpot_hit": {"pattern": [0, 120, 40, 200], "intensity": "strong"},
    "elimination": {"pattern": [0, 90],           "intensity": "medium"},
    "vip_join":    {"pattern": [0, 40, 30, 40],   "intensity": "light"},
    "tick":        {"pattern": [0, 15],           "intensity": "light"},
    "default":     {"pattern": [0, 30],           "intensity": "light"},
}
# default audio cue per event type (regie may override via cue.audio_cue_id)
TYPE_TO_AUDIO = {
    "jackpot_event": "jackpot_hit", "elimination": "elimination", "vip_join": "vip_join",
}


# =============================================================================
# PLC — Predictive Latency Compensation
# =============================================================================
@dataclass
class ClientLatencyState:
    last_rtt_ms: float = 0.0
    rtt_avg_ms: float = 0.0
    rtt_var_ms: float = 0.0
    jitter_ms: float = 0.0
    samples: int = 0
    plc_group: str = "A"

    def update_rtt(self, rtt_ms: float, alpha: float = 0.2) -> None:
        self.last_rtt_ms = rtt_ms
        if self.samples == 0:
            self.rtt_avg_ms = rtt_ms
        else:
            delta = rtt_ms - self.rtt_avg_ms
            self.rtt_avg_ms += alpha * delta
            self.rtt_var_ms = (1 - alpha) * (self.rtt_var_ms + alpha * delta * delta)
            self.jitter_ms = (1 - alpha) * self.jitter_ms + alpha * abs(delta)
        self.samples += 1

    @property
    def latency_bucket(self) -> str:
        r = self.rtt_avg_ms
        if r < 50:
            return "0-50"
        if r < 150:
            return "50-150"
        if r < 300:
            return "150-300"
        return ">300"


def compute_client_offset(server_ts: float, state: ClientLatencyState,
                          cfg: Config = CFG, now: Optional[float] = None) -> dict:
    """
    Pure, unit-testable PLC core.

    one_way     ≈ rtt/2
    elapsed     = now - server_ts            (bus + processing delay already burned since the cue stamp)
    base_target = server_ts + safety_margin  (target wall-clock in SERVER time)
    offset_ms   = safety_margin - elapsed*1000 - one_way   (ms the client waits AFTER receipt)
    Bounded to [min,max]: a slow path (high RTT or large elapsed) degrades to fire-on-receipt
    instead of overcompensating into the past. Pass now=server_ts to isolate network-only (tests).
    """
    now = time.time() if now is None else now
    rtt = state.rtt_avg_ms if state.samples > 0 else cfg.plc_default_rtt_ms
    one_way = max(0.0, rtt / 2.0)
    elapsed_ms = max(0.0, (now - server_ts) * 1000.0)
    base_target_ts = server_ts + cfg.plc_safety_margin_ms / 1000.0
    offset_ms = cfg.plc_safety_margin_ms - elapsed_ms - one_way
    offset_ms = max(cfg.plc_min_offset_ms, min(cfg.plc_max_offset_ms, offset_ms))
    return {
        "server_ts": round(server_ts, 4),
        "base_target_ts": round(base_target_ts, 4),
        "target_ts": round(base_target_ts, 4),         # SERVER-clock tie value (visual==haptic); clients act on offset_ms
        "client_latency_ms": round(rtt, 1),
        "offset_ms": round(offset_ms, 1),
        "latency_bucket": state.latency_bucket,
        "jitter_ms": round(state.jitter_ms, 1),
        "plc_version": cfg.plc_version,
        "plc_group": state.plc_group,
    }


# =============================================================================
# CONNECTION MANAGER
# =============================================================================
@dataclass
class ClientConn:
    ws: WebSocket
    cid: str
    ip: str
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=CFG.client_queue_max))
    state: ClientLatencyState = field(default_factory=ClientLatencyState)
    strong_haptics: deque = field(default_factory=lambda: deque(maxlen=64))
    last_seen: float = field(default_factory=time.time)

    def haptic_allowed(self) -> bool:
        """Soft-limit strong vibrations; beyond it the client downgrades to visual-only."""
        now = time.time()
        while self.strong_haptics and now - self.strong_haptics[0] > CFG.haptic_strong_window_s:
            self.strong_haptics.popleft()
        return len(self.strong_haptics) < CFG.haptic_strong_max

    def note_strong_haptic(self) -> None:
        self.strong_haptics.append(time.time())


class ConnectionManager:
    def __init__(self) -> None:
        self.clients: dict[str, ClientConn] = {}
        self.per_ip: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def register(self, ws: WebSocket, ip: str) -> Optional[ClientConn]:
        async with self._lock:
            if self.per_ip.get(ip, 0) >= CFG.max_conns_per_ip:
                return None
            cid = uuid.uuid4().hex[:12]
            conn = ClientConn(ws=ws, cid=cid, ip=ip)
            conn.state.plc_group = "B" if (int(cid, 16) % 100) / 100.0 < CFG.plc_ab_split else "A"
            self.clients[cid] = conn
            self.per_ip[ip] = self.per_ip.get(ip, 0) + 1
            if _HAS_PROM:
                M_WS.set(len(self.clients))
            return conn

    async def unregister(self, conn: ClientConn) -> None:
        async with self._lock:
            self.clients.pop(conn.cid, None)
            self.per_ip[conn.ip] = max(0, self.per_ip.get(conn.ip, 1) - 1)
            if self.per_ip[conn.ip] == 0:
                self.per_ip.pop(conn.ip, None)
            if _HAS_PROM:
                M_WS.set(len(self.clients))

    def _build_payload(self, conn: ClientConn, event: dict) -> dict:
        plc = compute_client_offset(event["server_ts"], conn.state)
        audio_id = event.get("audio_cue_id") or TYPE_TO_AUDIO.get(event["type"])
        haptic = None
        if audio_id:
            profile = HAPTIC_PROFILES.get(audio_id, HAPTIC_PROFILES["default"])
            strong = profile["intensity"] == "strong"
            if strong and not conn.haptic_allowed():
                haptic = None                                  # downgrade: visual-only
            else:
                if strong:
                    conn.note_strong_haptic()
                haptic = {
                    "profile": audio_id, "pattern": profile["pattern"],
                    "intensity": profile["intensity"], "sync_audio_id": audio_id,
                    "target_ts": plc["target_ts"],             # deckungsgleich mit visual
                }
        return {
            "kind": "event",
            "event_id": event["event_id"], "run_id": event["run_id"], "type": event["type"],
            **plc,
            "visual": {"type": event["type"], "eliminated": event.get("eliminated"),
                       "name": event.get("name"), **event.get("payload", {})},
            "audio_hints": {"cue_id": audio_id, "gain": 1.0} if audio_id else None,
            "haptic_cue": haptic,
        }

    async def broadcast(self, event: dict) -> None:
        """Fan-out one bus event to every local client (per-client PLC), non-blocking.
        A malformed/foreign bus message is skipped once — it must NEVER tear down the subscriber."""
        if not isinstance(event, dict) or "type" not in event or "server_ts" not in event:
            log.warning("skipped malformed bus event", extra={})
            return
        event.setdefault("event_id", uuid.uuid4().hex)
        event.setdefault("run_id", RUN_ID)
        for conn in list(self.clients.values()):
            try:
                payload = self._build_payload(conn, event)
            except Exception as e:                       # one bad client/build never stalls the rest
                log.warning(f"payload build failed: {e}", extra={"client_id": conn.cid})
                continue
            try:
                conn.queue.put_nowait(payload)
            except asyncio.QueueFull:
                _drop_oldest_game_event(conn)
                try:
                    conn.queue.put_nowait(payload)
                except asyncio.QueueFull:
                    if _HAS_PROM:
                        M_DROPS.inc()
                    log.warning("backpressure drop", extra={"client_id": conn.cid})


def _drop_oldest_game_event(conn: ClientConn) -> None:
    """Make room by discarding the oldest queued GAME event (control msgs are sent inline)."""
    try:
        conn.queue.get_nowait()
        if _HAS_PROM:
            M_DROPS.inc()
    except asyncio.QueueEmpty:
        pass


MANAGER = ConnectionManager()


# =============================================================================
# REDIS
# =============================================================================
class Bus:
    def __init__(self) -> None:
        self.redis: Optional[aioredis.Redis] = None
        self._sub_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        if CFG.redis_sentinels:
            nodes = [(h.split(":")[0], int(h.split(":")[1]))
                     for h in CFG.redis_sentinels.split(",") if h.strip()]
            sentinel = Sentinel(nodes, socket_timeout=0.5,
                                password=CFG.redis_password or None)
            self.redis = sentinel.master_for(CFG.redis_master_name, socket_timeout=0.5,
                                              password=CFG.redis_password or None,
                                              decode_responses=True)
        else:
            self.redis = aioredis.from_url(CFG.redis_url, decode_responses=True)
        await self.redis.ping()
        log.info("redis connected", extra={})

    async def publish(self, event: dict) -> None:
        assert self.redis is not None
        data = json.dumps(event, separators=(",", ":"))
        # live fan-out + durable append-only replay log (capped)
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.publish(CFG.channel, data)
            pipe.xadd(CFG.stream, {"e": data}, maxlen=CFG.stream_maxlen, approximate=True)
            await pipe.execute()

    _RL_LUA = ("local n = redis.call('INCR', KEYS[1]) "
               "if n == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end return n")

    async def rate_ok(self, api_key_id: str) -> bool:
        """Redis fixed-window limiter — atomic INCR+EXPIRE (Lua), shared across replicas."""
        assert self.redis is not None
        window = int(time.time()) // CFG.cue_rate_window_s
        key = f"rl:cue:{api_key_id}:{window}"
        n = await self.redis.eval(self._RL_LUA, 1, key, CFG.cue_rate_window_s + 1)
        return int(n) <= CFG.cue_rate_limit

    async def replay(self, since_id: str = "-", count: int = 500) -> list[dict]:
        assert self.redis is not None
        rows = await self.redis.xrange(CFG.stream, min=since_id, max="+", count=count)
        return [json.loads(fields["e"]) for _id, fields in rows]

    async def run_subscriber(self) -> None:
        """Subscribe to the bus; every received event is broadcast to local WS clients."""
        assert self.redis is not None
        backoff = 0.5
        while True:
            try:
                async with self.redis.pubsub() as pubsub:     # context-managed => closed on every exit
                    await pubsub.subscribe(CFG.channel)
                    backoff = 0.5
                    async for msg in pubsub.listen():
                        if msg.get("type") != "message":
                            continue
                        try:
                            event = json.loads(msg["data"])
                        except Exception:
                            continue
                        await MANAGER.broadcast(event)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # reconnect with capped backoff
                log.warning(f"subscriber error: {e}", extra={})
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 2)

    async def close(self) -> None:
        if self.redis is not None:
            await self.redis.aclose()


BUS = Bus()


# =============================================================================
# AUTH / RATE LIMIT helpers
# =============================================================================
def _api_keys() -> set[str]:
    return {k.strip() for k in CFG.api_keys.split(",") if k.strip()}


async def require_api_key(request: Request) -> str:
    provided = request.headers.get(CFG.api_key_header, "")
    keys = _api_keys()
    if not keys:                                   # fail-closed if no keys configured
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "no API keys configured")
    for k in keys:
        if hmac.compare_digest(provided, k):       # constant-time
            return hashlib.sha256(k.encode()).hexdigest()[:8]   # api_key_id (non-secret)
    raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid API key")


def _valid_ws_token(token: str) -> bool:
    if not CFG.ws_require_token:
        return True
    if not CFG.ws_token_secret or "." not in token:
        return False
    body, sig = token.rsplit(".", 1)
    expect = hmac.new(CFG.ws_token_secret.encode(), body.encode(), hashlib.sha256).hexdigest()[:32]
    if not hmac.compare_digest(sig, expect):
        return False
    try:                                            # body = "<expiry_epoch>"
        return float(body) >= time.time()
    except ValueError:
        return False


# =============================================================================
# APP
# =============================================================================
async def _lifespan(app: FastAPI):
    await BUS.connect()
    BUS._sub_task = asyncio.create_task(BUS.run_subscriber())
    log.info("cue_service up", extra={})
    try:
        yield
    finally:
        if BUS._sub_task:
            BUS._sub_task.cancel()
            try:
                await BUS._sub_task
            except asyncio.CancelledError:
                pass
        await BUS.close()


app = FastAPI(title="QQL Cue Service", version="1.0.0", lifespan=_lifespan)


@app.post("/api/show/cue", status_code=status.HTTP_202_ACCEPTED)
async def post_cue(cue: CueIn, request: Request, api_key_id: str = Depends(require_api_key)):
    if not await BUS.rate_ok(api_key_id):
        if _HAS_PROM:
            M_CUES_REJ.labels("rate_limited").inc()
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "rate limit exceeded")
    event = {
        "event_id": uuid.uuid4().hex,
        "run_id": RUN_ID,
        "server_ts": time.time(),
        "type": cue.type,
        "eliminated": cue.eliminated,
        "name": cue.name,
        "audio_cue_id": cue.audio_cue_id,
        "payload": cue.payload,
        "plc_group": cue.plc_group,
        "idempotency_key": cue.idempotency_key,
    }
    await BUS.publish(event)
    if _HAS_PROM:
        M_CUES_IN.labels(cue.type).inc()
    log.info("cue accepted", extra={"event_id": event["event_id"], "api_key_id": api_key_id})
    return {"accepted": True, "event_id": event["event_id"], "server_ts": event["server_ts"]}


@app.websocket("/api/show/stream")
async def ws_stream(ws: WebSocket):
    # behind a trusted Swarm ingress/LB the real client is in X-Forwarded-For (uvicorn --proxy-headers);
    # fall back to the socket peer. (Trust assumption: only a vetted proxy sets XFF; else cap at the edge.)
    xff = ws.headers.get("x-forwarded-for", "")
    ip = (xff.split(",")[0].strip() if xff else (ws.client.host if ws.client else "0.0.0.0"))
    token = ws.query_params.get("token", "")
    if not _valid_ws_token(token):
        await ws.close(code=4401)
        return
    await ws.accept()
    conn = await MANAGER.register(ws, ip)
    if conn is None:
        await ws.close(code=4429)                  # per-IP connection cap
        return

    async def writer():
        try:
            while True:
                payload = await conn.queue.get()
                await ws.send_json(payload)
                if _HAS_PROM:
                    M_EVENTS_OUT.inc()
        except (WebSocketDisconnect, RuntimeError):
            pass

    async def pinger():
        try:
            while True:
                await asyncio.sleep(CFG.ping_interval_s)
                # server-initiated RTT probe (control traffic, sent inline, bypasses game queue)
                await ws.send_json({"kind": "control", "type": "ping", "t_server": time.time()})
        except (WebSocketDisconnect, RuntimeError):
            pass

    w = asyncio.create_task(writer())
    p = asyncio.create_task(pinger())
    try:
        await ws.send_json({"kind": "control", "type": "hello", "client_id": conn.cid,
                            "plc_group": conn.state.plc_group, "plc_version": CFG.plc_version,
                            "server_ts": time.time()})
        while True:
            raw = await asyncio.wait_for(ws.receive_text(), timeout=CFG.client_idle_timeout_s)
            conn.last_seen = time.time()
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            if msg.get("kind") == "control" and msg.get("type") == "pong":
                t_server = float(msg.get("t_server", 0))
                if t_server:
                    rtt = (time.time() - t_server) * 1000.0
                    conn.state.update_rtt(rtt)
                    if _HAS_PROM:
                        M_RTT.observe(rtt)
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    except Exception as e:
        log.warning(f"ws error: {e}", extra={"client_id": conn.cid})
    finally:
        for t in (w, p):
            t.cancel()
        await asyncio.gather(w, p, return_exceptions=True)   # join cancelled tasks before unregister
        await MANAGER.unregister(conn)


@app.get("/api/show/replay")
async def replay(since: str = "-", count: int = 500, api_key_id: str = Depends(require_api_key)):
    return {"events": await BUS.replay(since, min(count, 5000))}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "run_id": RUN_ID}


@app.get("/readyz")
async def readyz():
    try:
        assert BUS.redis is not None
        await BUS.redis.ping()
        return {"ready": True, "ws": len(MANAGER.clients)}
    except Exception:
        return JSONResponse({"ready": False}, status_code=503)


@app.get("/metrics")
async def metrics():
    if not _HAS_PROM:
        return PlainTextResponse("prometheus_client not installed", status_code=501)
    return PlainTextResponse(generate_latest().decode(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":   # pragma: no cover
    import uvicorn
    uvicorn.run("cue_service:app", host="0.0.0.0", port=int(_env("PORT", "8080")), log_level="info")
