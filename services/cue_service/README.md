# QQL Cue Service — Realtime Nervous System

The glue between the show's control plane (regie / Twitch extension / game engine) and every
viewer device. A cue is POSTed once; the service fans it out over Redis to all service replicas,
and each replica pushes a **per-client latency-compensated** combined event (visual + audio hint +
haptic cue) so the effect lands in sync across phones and browsers.

```
 regie/engine ──POST /api/show/cue──▶ cue_service ──PUBLISH──▶ Redis (Sentinel) ──▶ every replica
                                          │  XADD show:events:log (replay)              │ subscribe
   viewer device ◀──WS /api/show/stream──────────── per-client PLC + haptic build ◀─────┘
```

## How it works (architecture)
The **control plane** and **data plane** are deliberately split. `POST /api/show/cue` is authenticated
(API key) and rate-limited — only the show's brain may fire cues. `WS /api/show/stream` is the public
viewer fan-out, protected by per-IP connection caps and an optional HMAC viewer token, never an API key
(viewers are anonymous). Inside Redis we use **Pub/Sub** for the low-latency live path (every replica
gets every event and pushes to its own sockets, so we scale horizontally just by adding replicas) and a
capped **Stream** (`XADD`) as an append-only **replay log** for post-show analysis (`GET /api/show/replay`).

**Predictive Latency Compensation (PLC).** Each WS client carries a rolling RTT estimate, measured by
server-initiated `ping`→`pong` probes (EWMA average + variance + jitter + a coarse latency bucket). For
every event we compute, per client, `base_target_ts = server_ts + safety_margin` and an `offset_ms`
(how long the client waits after receiving before firing) so transmission + wait ≈ the same wall-clock
moment for everyone. The offset is **bounded**: a 900 ms-RTT mobile client degrades cleanly to
*fire-on-receipt* instead of overcompensating into the past. `plc_group`/`plc_version` let us
**dark-launch** a new PLC algorithm to a fraction of clients (`PLC_AB_SPLIT`) and A/B it live.

**Haptics, synced to audio.** Each event maps an `audio_cue_id` to a haptic profile; the `haptic_cue`
carries the *same* `target_ts` as the visual cue (sync guarantee) plus `sync_audio_id`. A per-client
**backoff** caps strong vibrations (default ≤3 per 10 s) and downgrades to visual-only beyond that, so
a jackpot storm doesn't turn a phone into a buzzer. Honest scope: this drives the Web `navigator.vibrate()`
API — reliable on Android Chrome, **ignored by iOS Safari** — it's a cosmetic sync cue, not a hardware driver.

**Resilience built in before it's needed.** Bounded per-client send queues with an oldest-game-event drop
policy (control traffic is never dropped); Redis subscriber auto-reconnect with capped backoff; idle-socket
timeouts; constant-time API-key compare; fail-closed auth (no keys configured ⇒ 503); structured JSON logs
correlated by `run_id`/`event_id`/`client_id`; Prometheus metrics at `/metrics`; `/healthz` + `/readyz`.

## Client-side contract (browser / mobile-web)
```js
const ws = new WebSocket(`wss://YOUR_HOST/api/show/stream`);     // add ?token=... if WS_REQUIRE_TOKEN
ws.onmessage = (m) => {
  const e = JSON.parse(m.data);
  if (e.kind === "control" && e.type === "ping") {               // RTT probe: echo it straight back
    ws.send(JSON.stringify({ kind: "control", type: "pong", t_server: e.t_server }));
    return;
  }
  if (e.kind !== "event") return;
  const fire = () => {
    renderVisual(e.visual);                                       // your HUD/grid renderer
    if (e.haptic_cue && navigator.vibrate) navigator.vibrate(e.haptic_cue.pattern);
  };
  setTimeout(fire, Math.max(0, e.offset_ms));                    // PLC: wait the per-client offset, then fire in sync
};
```

## Scaling model (Docker Swarm on Hetzner)
Swarm does not autoscale natively, so scaling is metric-driven via an external loop. Thresholds:

| Signal (per replica, 60 s window) | Action |
|---|---|
| CPU > 80% **or** `qql_ws_connections` > 8000 | `docker service scale qql_cue_service=+1` |
| CPU < 30% **and** connections < 2000 (sustained 5 min) | scale −1 (floor 2) |
| `qql_client_drops_total` rising | scale up + raise `CLIENT_QUEUE_MAX` |
| Sentinel reports master down | automatic failover (no app action; subscriber reconnects) |

Autoscaler sketch (cron/sidecar, reads Prometheus, calls the Swarm API):
```python
while True:
    cpu = prom("avg(rate(container_cpu...{service='qql_cue_service'}[1m]))")
    conns = prom("sum(qql_ws_connections)")
    reps = swarm_replicas("qql_cue_service")
    if cpu > 0.8 or conns / max(reps,1) > 8000: swarm_scale("qql_cue_service", reps + 1)
    elif cpu < 0.3 and conns / max(reps,1) < 2000: swarm_scale("qql_cue_service", max(2, reps - 1))
    sleep(60)
```

## Chaos & test sketches (staging only)
- **Kill a replica:** `docker service update --force qql_cue_service` mid-show → viewers reconnect, no event loss (Pub/Sub is at-most-once live, but the Stream replay backfills gaps).
- **Redis master kill:** `docker kill <redis-master task>` → Sentinel promotes a replica; assert subscriber reconnects within backoff and cues resume.
- **Latency injection:** `tc qdisc add dev eth0 root netem delay 250ms 50ms` on a worker → assert PLC offsets shrink toward the floor and clients still fire within tolerance.
- **Backpressure:** a slow client (artificially blocked socket) → assert oldest game events drop, control traffic survives, other clients unaffected.
- **Unit:** `pytest services/cue_service/tests` covers PLC bounds, high-latency degrade, latency buckets, haptic backoff.

## Run locally
```bash
docker run -p 6379:6379 redis:7                       # dev Redis (single node)
cd services/cue_service && pip install -r requirements.txt
API_KEYS=devkey REDIS_URL=redis://localhost:6379/0 python cue_service.py
# fire a cue:
curl -XPOST localhost:8080/api/show/cue -H 'X-API-Key: devkey' \
  -H 'Content-Type: application/json' -d '{"type":"jackpot_event"}'
```

> Compliance: `jackpot_event` is a **NON-PAYOUT** cosmetic hype identifier (no money/pool/payout). The
> service moves event signals only — it never holds value. Secrets come from env or `/run/secrets/*`,
> never from files in git.
