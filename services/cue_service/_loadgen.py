"""
QQL load-gen — N fake viewers that do the real ping/pong RTT handshake so the
server measures each client's latency and we can SEE Predictive Latency Compensation.
Each viewer simulates its own one-way network latency, so PLC offsets fan out:
high-latency viewers get a SMALLER offset (wait less) to stay in sync. NON-PAYOUT demo.
"""
import asyncio
import json
import random

import websockets

URL = "ws://127.0.0.1:8080/api/show/stream"
N = 50


async def viewer(idx: int, owl_ms: float, buf: list) -> None:
    delay = owl_ms / 1000.0                                   # simulated one-way latency
    try:
        async with websockets.connect(URL, open_timeout=6) as ws:
            async for raw in ws:
                m = json.loads(raw)
                kind, typ = m.get("kind"), m.get("type")
                if kind == "control" and typ == "ping":
                    await asyncio.sleep(delay)                # delay the pong => server sees this RTT
                    await ws.send(json.dumps({"kind": "control", "type": "pong",
                                              "t_server": m["t_server"]}))
                elif kind == "event":
                    buf.append((m.get("client_latency_ms"), m.get("offset_ms"), m.get("type"), owl_ms))
    except Exception:
        pass


async def reporter(buf: list) -> None:
    seen = 0
    while True:
        await asyncio.sleep(1.0)
        if len(buf) > seen:
            batch = buf[seen:]
            seen = len(buf)
            offs = sorted(o for _, o, _, _ in batch if o is not None)
            rtts = sorted(r for r, _, _, _ in batch if r is not None)
            if not offs:
                continue
            ty = batch[-1][2]

            def pct(a, q):
                return a[min(len(a) - 1, int(q * len(a)))]

            print(f"\n=== EVENT '{ty}' -> fan-out to {len(batch)} viewers ===")
            print(f"  measured RTT(ms)   min={rtts[0]:.0f}  p50={pct(rtts, 0.5):.0f}  max={rtts[-1]:.0f}")
            print(f"  PLC offset(ms)     min={offs[0]:.0f}  p50={pct(offs, 0.5):.0f}  max={offs[-1]:.0f}"
                  f"   (hi-latency viewers wait LESS -> all fire in sync)")
            samples = sorted(batch, key=lambda x: x[3])
            for j in (0, len(samples) // 2, -1):
                r, o, _, owl = samples[j]
                print(f"    sample one-way~{owl:.0f}ms -> server RTT {r:.0f}ms -> waits offset {o:.0f}ms")


async def main() -> None:
    buf: list = []
    print(f"Connecting {N} fake viewers to {URL} (each with a simulated one-way latency 5-250ms) ...")
    tasks = [asyncio.create_task(viewer(i, random.uniform(5, 250), buf)) for i in range(N)]
    tasks.append(asyncio.create_task(reporter(buf)))
    print("Connected. Server pings every ~5s -> RTT stabilises in 5-10s. Fire a cue to see PLC.\n")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
