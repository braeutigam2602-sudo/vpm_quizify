#!/usr/bin/env python3
"""
QQL · MASTER GENESIS — Asset Ingest & Register Pipeline
=======================================================
Per-theme, per-layer asset lifecycle for the OBS compositing stack.

FLOW (per manifest entry):
  1. Is the asset already in Supabase Storage (by destination path)?
       YES -> download/verify -> INGEST & REGISTER
       NO  -> LOCAL generation hook (Ollama/ComfyUI via $LOCAL_GENERATOR_CMD)
              -> validate -> INGEST & REGISTER   (NO paid APIs, ever)
  2. QA every asset: alpha-channel correctness, resolution, fps, file size, hash uniqueness,
     and — for stinger assets — the Opus audio cue (codec=opus, 48kHz, mono/stereo, bitrate).
  3. Only QA-passing assets are flagged `active=true` for OBS (previous active is deprecated).
     A stinger that declares an audio cue but fails Opus/bitrate validation is NOT activated.

NOTE: 'Opus' = the audio codec (RFC 6716). It is unrelated to the Claude model name 'Opus 4.8'.

COST DISCIPLINE: no paid generation APIs. We build the power plant; we don't rent the grid.
SECURITY: secrets come from ENV only — never written to disk, never logged.

ENV:
  SUPABASE_URL                  https://<ref>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY     service-role JWT (backend only)
  SUPABASE_BUCKET               storage bucket (default: assets)
  ASSET_SRC_DIR                 local asset root (default: assets)
  LOCAL_GENERATOR_CMD           optional; e.g. "python tools/comfy_gen.py {out} {theme} {layer} {variant}"
  DRY_RUN                       true => validate only, no upload/insert (default: false)
  QA_MAX_MB                     max file size in MB (default: 400)

USAGE:  python qql_master_genesis.py --theme jackpot_ps5
"""
from __future__ import annotations
import argparse, hashlib, json, logging, mimetypes, os, re, subprocess, sys
from pathlib import Path

import requests

try:
    from PIL import Image
    HAS_PIL = True
except Exception:
    HAS_PIL = False

LOG = logging.getLogger("genesis")
VALID_LAYERS = {"0", "1", "2", "3", "3.5", "4"}
STINGER_LAYERS = {"3", "3.5"}                  # FX/stinger layers MUST carry an Opus audio cue (opt out via expect.audio_required:false)
SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")   # theme/variant: no shell metachars (defense for the local-gen hook)

# ── config (ENV only) ────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
BUCKET       = os.environ.get("SUPABASE_BUCKET", "assets")
SRC_DIR      = Path(os.environ.get("ASSET_SRC_DIR", "assets"))
GEN_CMD      = os.environ.get("LOCAL_GENERATOR_CMD", "")
DRY_RUN      = os.environ.get("DRY_RUN", "false").lower() in ("1", "true", "yes")
QA_MAX_MB    = float(os.environ.get("QA_MAX_MB", "400"))


def _auth_headers(extra: dict | None = None) -> dict:
    h = {"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"}
    if extra:
        h.update(extra)
    return h


# ── helpers ──────────────────────────────────────────────────────────────────
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def probe(path: Path, expect: dict) -> dict:
    """Extract width/height/alpha/fps/mime/size. Images via Pillow; video via ffprobe if present."""
    size = path.stat().st_size
    mime = expect.get("mime") or mimetypes.guess_type(str(path))[0] or "application/octet-stream"
    meta = {"mime_type": mime, "size": size, "width": None, "height": None,
            "alpha_present": None,  # None = NOT yet measured; QA fails alpha-required if it stays unmeasured
            "fps": expect.get("fps"),
            "duration_ms": expect.get("duration_ms"), "audio_codec": expect.get("audio_codec")}

    if mime.startswith("image/") and HAS_PIL:
        try:
            with Image.open(path) as im:
                meta["width"], meta["height"] = im.size
                meta["alpha_present"] = (im.mode in ("RGBA", "LA")) or ("transparency" in im.info)
        except Exception as e:
            LOG.warning("Pillow probe failed for %s: %s", path.name, e)

    if mime.startswith("video/"):
        info = _ffprobe(path)
        if info:
            meta.update({k: v for k, v in info.items() if v is not None})

    return meta


def _ffprobe(path: Path) -> dict | None:
    """Best-effort video probe; returns {} silently if ffprobe is unavailable."""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height,r_frame_rate,pix_fmt",
             "-show_entries", "format=duration", "-of", "json", str(path)],
            capture_output=True, text=True, timeout=60,
        )
        if out.returncode != 0:
            return None
        data = json.loads(out.stdout or "{}")
        st = (data.get("streams") or [{}])[0]
        num, _, den = (st.get("r_frame_rate") or "0/1").partition("/")
        fps = round(float(num) / float(den)) if float(den or 0) else None
        dur = data.get("format", {}).get("duration")
        return {"width": st.get("width"), "height": st.get("height"), "fps": fps,
                "alpha_present": "a" in (st.get("pix_fmt") or ""),  # yuva*/rgba => alpha
                "duration_ms": int(float(dur) * 1000) if dur else None}
    except FileNotFoundError:
        LOG.info("ffprobe unavailable — alpha/fps will be treated as unmeasured for %s", path.name)
        return None
    except Exception as e:
        LOG.warning("ffprobe error for %s: %s", path.name, e)
        return None


def qa(meta: dict, expect: dict, path: Path) -> list[str]:
    """Return list of QA failures. Empty list => asset is fit to go active."""
    fails: list[str] = []
    if meta["size"] > QA_MAX_MB * 1024 * 1024:
        fails.append(f"file too large: {meta['size']/1e6:.1f}MB > {QA_MAX_MB}MB")
    if expect.get("alpha") and meta.get("alpha_present") is not True:
        fails.append("alpha channel REQUIRED but not verified/absent (install Pillow/ffprobe to measure)")
    for dim in ("width", "height"):
        want = expect.get(dim)
        if want and meta.get(dim) and meta[dim] < want:
            fails.append(f"{dim} {meta[dim]} < required {want}")
    want_fps = expect.get("fps")
    if want_fps and meta.get("fps") and abs(meta["fps"] - want_fps) > 1:
        fails.append(f"fps {meta['fps']} != required {want_fps}")
    return fails


def safe_join(base: Path, rel: str) -> Path | None:
    """Join rel under base, refusing absolute paths / '..' escapes (path-traversal guard)."""
    if not rel or rel.startswith(("/", "\\")) or ".." in rel.replace("\\", "/").split("/"):
        return None
    p = base / rel
    try:
        p.resolve().relative_to(base.resolve())
    except Exception:
        return None
    return p


def probe_audio(path: Path) -> dict | None:
    """ffprobe the first audio stream. None => ffprobe missing/unreadable; {'codec':None} => no audio stream."""
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name,sample_rate,channels,bit_rate",
             "-show_entries", "format=duration,bit_rate", "-of", "json", str(path)],
            capture_output=True, text=True, timeout=60,
        )
        if out.returncode != 0:
            return None
        data = json.loads(out.stdout or "{}")
        streams = data.get("streams") or []
        if not streams:
            return {"codec": None}                       # container carries NO audio stream
        st, fmt = streams[0], data.get("format", {})
        br = st.get("bit_rate") or fmt.get("bit_rate")
        dur = fmt.get("duration")
        return {
            "codec": st.get("codec_name"),
            "sample_rate": int(st["sample_rate"]) if str(st.get("sample_rate") or "").isdigit() else None,
            "channels": st.get("channels"),
            "bitrate_kbps": round(int(br) / 1000) if str(br or "").isdigit() else None,
            "duration_ms": int(float(dur) * 1000) if dur else None,
        }
    except FileNotFoundError:
        LOG.info("ffprobe unavailable — audio treated as unmeasured for %s", path.name)
        return None
    except Exception as e:
        LOG.warning("audio probe error for %s: %s", path.name, e)
        return None


def qa_audio(ameta: dict | None, expect_audio: dict) -> list[str]:
    """Validate the Opus stinger cue. 'Opus' = audio codec RFC 6716 (NOT the Claude model name)."""
    if ameta is None:
        return ["audio REQUIRED but could not be probed (install ffprobe to measure)"]
    if not ameta.get("codec"):
        return ["audio REQUIRED but the file has no audio stream"]
    fails: list[str] = []
    want_codec = (expect_audio.get("codec") or "opus").lower()
    if (ameta.get("codec") or "").lower() != want_codec:
        fails.append(f"audio codec {ameta.get('codec')!r} != required {want_codec!r}")
    want_sr = expect_audio.get("sample_rate", 48000)
    if want_sr and ameta.get("sample_rate") != want_sr:
        fails.append(f"audio sample_rate {ameta.get('sample_rate')} != required {want_sr}Hz")
    chans = expect_audio.get("channels", [1, 2])
    chans = [chans] if isinstance(chans, int) else list(chans)
    if ameta.get("channels") not in chans:
        fails.append(f"audio channels {ameta.get('channels')} not in allowed {chans}")
    min_br = expect_audio.get("min_bitrate_kbps")
    if min_br:
        if ameta.get("bitrate_kbps") is None:
            fails.append(f"audio bitrate unmeasurable but min {min_br}kbps required")
        elif ameta["bitrate_kbps"] < min_br:
            fails.append(f"audio bitrate {ameta['bitrate_kbps']}kbps < required {min_br}kbps")
    return fails


# ── Supabase Storage ─────────────────────────────────────────────────────────
def storage_path(theme: str, entry: dict) -> str:
    ext = Path(entry["file"]).suffix
    return f"{theme}/layer{entry['layer']}/{entry.get('variant','default')}.v{entry.get('version',1)}{ext}"


def storage_exists(dest: str) -> bool:
    r = requests.get(f"{SUPABASE_URL}/storage/v1/object/info/{BUCKET}/{dest}",
                     headers=_auth_headers(), timeout=30)
    return r.status_code == 200


def storage_upload(local: Path, dest: str, mime: str) -> str:
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{dest}"
    with local.open("rb") as f:
        r = requests.post(url, headers=_auth_headers({"Content-Type": mime, "x-upsert": "true"}),
                          data=f, timeout=300)
    r.raise_for_status()
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{dest}"   # public/CDN url


# ── Supabase DB (PostgREST) ──────────────────────────────────────────────────
def db_deprecate_active(theme: str, layer: str, variant: str) -> None:
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/assets",
        headers=_auth_headers({"Content-Type": "application/json", "Prefer": "return=minimal"}),
        params={"theme": f"eq.{theme}", "layer": f"eq.{layer}", "variant": f"eq.{variant}", "active": "eq.true"},
        data=json.dumps({"active": False, "deprecated": True}), timeout=60,
    ).raise_for_status()


def db_register(row: dict) -> None:
    # upsert on the (theme,layer,variant,version) unique index => re-runs UPDATE the same row (idempotent)
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/assets",
        headers=_auth_headers({"Content-Type": "application/json",
                               "Prefer": "resolution=merge-duplicates,return=minimal"}),
        params={"on_conflict": "theme,layer,variant,version"},
        data=json.dumps(row), timeout=60,
    )
    r.raise_for_status()


def db_activate(theme: str, layer: str, variant: str, version: int) -> None:
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/assets",
        headers=_auth_headers({"Content-Type": "application/json", "Prefer": "return=minimal"}),
        params={"theme": f"eq.{theme}", "layer": f"eq.{layer}",
                "variant": f"eq.{variant}", "version": f"eq.{version}"},
        data=json.dumps({"active": True}), timeout=60,
    ).raise_for_status()


# ── local generation hook (NO paid API) ──────────────────────────────────────
def generate_local(out: Path, theme: str, entry: dict) -> bool:
    if not GEN_CMD:
        LOG.error("Asset missing and LOCAL_GENERATOR_CMD not set — cannot generate %s", out.name)
        return False
    cmd = GEN_CMD.format(out=str(out), theme=theme, layer=entry["layer"],
                         variant=entry.get("variant", "default"))
    LOG.info("Local generation: %s", cmd)
    out.parent.mkdir(parents=True, exist_ok=True)
    rc = subprocess.run(cmd, shell=True, timeout=int(os.environ.get("GEN_TIMEOUT", "1800"))).returncode
    if rc != 0 or not out.exists():
        LOG.error("Local generator failed (rc=%s) for %s", rc, out.name)
        return False
    return True


# ── per-entry pipeline ───────────────────────────────────────────────────────
def process_entry(theme: str, entry: dict) -> bool:
    layer = str(entry.get("layer", ""))
    if layer not in VALID_LAYERS:
        LOG.error("entry %s has invalid layer %r (allowed: %s)", entry.get("file"), layer, sorted(VALID_LAYERS))
        return False
    variant = entry.get("variant", "default")
    fname = str(entry.get("file", ""))
    if not SAFE_NAME.match(theme) or not SAFE_NAME.match(str(variant)):
        LOG.error("unsafe theme/variant (allowed [A-Za-z0-9_.-]): %r / %r", theme, variant)
        return False
    if (not fname) or ("/" in fname) or ("\\" in fname) or (".." in fname):
        LOG.error("unsafe asset file path (no separators or '..'): %r", fname)
        return False
    version = int(entry.get("version", 1))
    local = SRC_DIR / theme / fname
    dest = storage_path(theme, entry)
    expect = entry.get("expect", {})

    # 1) presence: storage is authoritative -> ingest; else local file; else local generator
    if storage_exists(dest):
        LOG.info("[%s/L%s/%s] already in storage", theme, layer, variant)
        if not local.exists():
            r = requests.get(f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{dest}",
                             headers=_auth_headers(), timeout=300)
            if not r.ok:   # do NOT silently regenerate a stored asset on a transient download error
                LOG.error("[%s/L%s/%s] storage download failed: HTTP %s", theme, layer, variant, r.status_code)
                return False
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(r.content)
    elif not local.exists():
        LOG.warning("[%s/L%s/%s] not in storage and not local — invoking local generator", theme, layer, variant)
        if not generate_local(local, theme, entry):
            return False

    # 2) QA — visual
    meta = probe(local, expect)
    fails = qa(meta, expect, local)
    if fails:
        LOG.error("[%s/L%s/%s] QA FAILED: %s", theme, layer, variant, "; ".join(fails))
        return False

    # 2b) QA — Opus audio cue. Stinger/FX assets MUST carry a valid Opus cue, else NOT activated.
    audio_ref = entry.get("audio_cue_ref")
    expect_audio = expect.get("audio")
    audio_required = expect.get("audio_required")
    if audio_required is None:                            # default: stinger 'type' or FX layers require audio
        audio_required = (entry.get("type") == "stinger") or (layer in STINGER_LAYERS)
    audio_rec, apath = None, None
    if audio_required and not (audio_ref or expect_audio):
        LOG.error("[%s/L%s/%s] stinger requires an Opus audio cue but manifest declares no audio_cue_ref/expect.audio",
                  theme, layer, variant)
        return False                                      # a stinger can NOT go active without a declared cue
    if audio_ref or expect_audio:
        if not audio_ref:
            LOG.error("[%s/L%s/%s] expect.audio declared but audio_cue_ref is missing", theme, layer, variant)
            return False
        apath = safe_join(SRC_DIR / theme, str(audio_ref))
        if apath is None:
            LOG.error("[%s/L%s/%s] unsafe audio_cue_ref (no '..'/absolute): %r", theme, layer, variant, audio_ref)
            return False
        ameta = probe_audio(apath) if apath.exists() else None
        afails = qa_audio(ameta, expect_audio or {})
        if afails:
            LOG.error("[%s/L%s/%s] AUDIO QA FAILED: %s", theme, layer, variant, "; ".join(afails))
            return False                                  # missing/wrong Opus cue => NOT activated
        audio_rec = {"ref": audio_ref, **(ameta or {})}
        LOG.info("[%s/L%s/%s] AUDIO OK  codec=%s %sHz %sch %skbps", theme, layer, variant,
                 ameta.get("codec"), ameta.get("sample_rate"), ameta.get("channels"), ameta.get("bitrate_kbps"))

    digest = sha256_file(local)
    LOG.info("[%s/L%s/%s] QA OK  %sx%s alpha=%s fps=%s %.1fMB sha=%s%s",
             theme, layer, variant, meta["width"], meta["height"], meta["alpha_present"],
             meta["fps"], meta["size"] / 1e6, digest[:12], "  +opus" if audio_rec else "")

    if DRY_RUN:
        LOG.info("[DRY_RUN] would upload+register %s%s", dest, " (+audio cue)" if audio_rec else "")
        return True

    # 3) upload, register INACTIVE (avoids 'one active' index collision), then flip active
    cdn = storage_upload(local, dest, meta["mime_type"])
    if audio_rec and apath is not None:                   # upload the Opus cue alongside the visual
        adest = f"{theme}/audio/{apath.name}"
        amime = mimetypes.guess_type(apath.name)[0] or "audio/ogg"
        audio_rec["url"] = storage_upload(apath, adest, amime)
    db_register({
        "theme": theme, "layer": layer, "variant": variant, "version": version,
        "url": dest, "cdn_url": cdn, "mime_type": meta["mime_type"],
        "width": meta["width"], "height": meta["height"], "fps": meta["fps"],
        "alpha_present": bool(meta["alpha_present"]), "hash_sha256": digest,
        "audio_codec": (audio_rec or {}).get("codec") or meta.get("audio_codec"),
        "duration_ms": meta.get("duration_ms") or (audio_rec or {}).get("duration_ms"),
        "active": False, "deprecated": False, "created_by": "genesis",
        "meta": {"qa": "pass", **({"audio": audio_rec} if audio_rec else {})},
    })
    db_deprecate_active(theme, layer, variant)          # retire the previous active version
    try:
        db_activate(theme, layer, variant, version)     # promote this version to active
    except Exception:
        LOG.error("[%s/L%s/%s] ACTIVATION FAILED — layer left with NO active asset; re-run pipeline to recover",
                  theme, layer, variant)
        raise
    LOG.info("[%s/L%s/%s] REGISTERED active v%s -> %s", theme, layer, variant, version, cdn)
    return True


def load_manifest(theme: str) -> list[dict]:
    mf = SRC_DIR / theme / "manifest.json"
    if not mf.exists():
        raise FileNotFoundError(f"manifest not found: {mf}")
    data = json.loads(mf.read_text(encoding="utf-8"))
    return data["assets"] if isinstance(data, dict) else data


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="QQL Master Genesis — asset ingest & register")
    ap.add_argument("--theme", default=os.environ.get("THEME", "jackpot_ps5"))
    args = ap.parse_args()

    if not SUPABASE_URL or not SERVICE_KEY:
        LOG.error("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required (env).")
        return 2
    if not SAFE_NAME.match(args.theme):
        LOG.error("unsafe --theme (allowed [A-Za-z0-9_.-]): %r", args.theme)
        return 2

    LOG.info("Genesis start · theme=%s · bucket=%s · dry_run=%s", args.theme, BUCKET, DRY_RUN)
    try:
        entries = load_manifest(args.theme)
    except Exception as e:
        LOG.error("manifest error: %s", e)
        return 2

    ok = sum(process_entry(args.theme, e) for e in entries)
    total = len(entries)
    LOG.info("Genesis done · %s/%s assets passed", ok, total)
    return 0 if ok == total else 1


if __name__ == "__main__":
    sys.exit(main())
