-- ============================================================================
-- QQL · Quizify Quantum Live — PRODUCTION SCHEMA (Supabase / Postgres 15+)
-- ----------------------------------------------------------------------------
-- IDEMPOTENT: safe to run N times (IF NOT EXISTS / OR REPLACE / DROP..IF EXISTS).
-- TRANSACTIONAL: applied by db_deploy.yml via `psql --single-transaction -v ON_ERROR_STOP=1`.
-- Compliance: theme 'jackpot_ps5' is a cosmetic NON-PAYOUT hype-theme IDENTIFIER
--             (no money/pool/payout). The word 'jackpot' here is a code label only.
-- assets.layer 0..4 maps to the OBS compositing layers:
--   0 = photoreal 8K studio plate · 1 = grid/HUD overlay · 2 = host · 3/3.5 = FX · 4 = top chrome
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()

-- ── players ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS players (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  handle       text NOT NULL,
  display_name text NOT NULL,
  avatar_url   text,
  is_vip       boolean NOT NULL DEFAULT false,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE players ADD COLUMN IF NOT EXISTS is_vip boolean NOT NULL DEFAULT false;
CREATE UNIQUE INDEX IF NOT EXISTS players_handle_ux ON players (lower(handle));

-- ── show_state (singleton row id=1) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS show_state (
  id            integer PRIMARY KEY DEFAULT 1,
  state         text NOT NULL DEFAULT 'idle',
  scene_profile text NOT NULL DEFAULT 'default',
  theme         text NOT NULL DEFAULT 'jackpot_ps5',
  updated_by    text,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT show_state_singleton CHECK (id = 1)
);
INSERT INTO show_state (id, state, scene_profile, theme)
VALUES (1, 'idle', 'default', 'jackpot_ps5')
ON CONFLICT (id) DO NOTHING;

-- ── assets (versioned, one active per theme/layer/variant) ───────────────────
CREATE TABLE IF NOT EXISTS assets (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  theme         text NOT NULL,
  layer         text NOT NULL CHECK (layer IN ('0','1','2','3','3.5','4')),
  variant       text NOT NULL DEFAULT 'default',
  version       integer NOT NULL DEFAULT 1,
  url           text NOT NULL,
  cdn_url       text,
  mime_type     text NOT NULL,
  width         integer,
  height        integer,
  fps           integer,
  alpha_present boolean NOT NULL DEFAULT false,
  hash_sha256   text,
  audio_codec   text,
  duration_ms   integer,
  active        boolean NOT NULL DEFAULT false,
  deprecated    boolean NOT NULL DEFAULT false,
  readonly      boolean NOT NULL DEFAULT false,
  created_at    timestamptz NOT NULL DEFAULT now(),
  created_by    text,
  last_used_at  timestamptz,
  meta          jsonb DEFAULT '{}'::jsonb
);
-- exactly ONE active asset per (theme, layer, variant)
CREATE UNIQUE INDEX IF NOT EXISTS assets_one_active_ux  ON assets (theme, layer, variant) WHERE active;
-- no duplicate version rows
CREATE UNIQUE INDEX IF NOT EXISTS assets_version_ux     ON assets (theme, layer, variant, version);
-- fast lookup of the active asset for a (theme, layer)
CREATE INDEX        IF NOT EXISTS assets_lookup_ix      ON assets (theme, layer, active);
-- content-hash uniqueness (QA: reject byte-identical re-uploads)
CREATE UNIQUE INDEX IF NOT EXISTS assets_hash_ux        ON assets (hash_sha256) WHERE hash_sha256 IS NOT NULL;

-- ── show_events (append-only cue bus — Master-Bible §5) ──────────────────────
CREATE TABLE IF NOT EXISTS show_events (
  id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  ts              timestamptz NOT NULL DEFAULT now(),
  type            text NOT NULL CHECK (type IN ('action','elimination','ad_break','jackpot_event','vip_join','reset','lobby')),
  payload         jsonb NOT NULL DEFAULT '{}'::jsonb,
  source          text,                 -- e.g. 'n8n:pulse', 'engine', 'manual'
  idempotency_key text UNIQUE           -- dedupe (n8n passes a stable key)
);
CREATE INDEX IF NOT EXISTS show_events_ts_ix ON show_events (ts DESC);

-- ── updated_at auto-touch trigger ────────────────────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$;
DROP TRIGGER IF EXISTS trg_players_updated ON players;
CREATE TRIGGER trg_players_updated BEFORE UPDATE ON players
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
DROP TRIGGER IF EXISTS trg_show_state_updated ON show_state;
CREATE TRIGGER trg_show_state_updated BEFORE UPDATE ON show_state
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================================
-- ROW LEVEL SECURITY
--   anon/authenticated (the public frontend) may READ safe rows only.
--   service_role (the backend) BYPASSES RLS and does all writes.
--   show_events is backend-only (no anon policy => denied for anon).
-- ============================================================================
ALTER TABLE players     ENABLE ROW LEVEL SECURITY;
ALTER TABLE show_state  ENABLE ROW LEVEL SECURITY;
ALTER TABLE assets      ENABLE ROW LEVEL SECURITY;
ALTER TABLE show_events ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS players_public_read ON players;
CREATE POLICY players_public_read ON players
  FOR SELECT TO anon, authenticated USING (true);

DROP POLICY IF EXISTS show_state_public_read ON show_state;
CREATE POLICY show_state_public_read ON show_state
  FOR SELECT TO anon, authenticated USING (true);

-- public sees only the ACTIVE, non-deprecated assets (what OBS should load)
DROP POLICY IF EXISTS assets_public_read ON assets;
CREATE POLICY assets_public_read ON assets
  FOR SELECT TO anon, authenticated USING (active = true AND deprecated = false);

-- ── grants (RLS still filters rows; grants gate the verbs) ────────────────────
GRANT USAGE ON SCHEMA public TO anon, authenticated;
GRANT SELECT ON players, show_state, assets TO anon, authenticated;   -- show_events intentionally NOT granted to anon
GRANT ALL ON ALL TABLES    IN SCHEMA public TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;

-- ── helper view: the current active asset set for a theme (frontend convenience)
CREATE OR REPLACE VIEW active_assets AS
  SELECT theme, layer, variant, version, url, cdn_url, mime_type,
         width, height, fps, alpha_present, duration_ms
  FROM assets
  WHERE active = true AND deprecated = false;
GRANT SELECT ON active_assets TO anon, authenticated;

COMMENT ON TABLE  assets       IS 'Versioned OBS compositing assets; layer 0..4. NON-PAYOUT themes.';
COMMENT ON TABLE  show_events  IS 'Append-only cue bus: action|elimination|ad_break|jackpot_event(cosmetic)|vip_join. Backend-only (RLS).';
COMMENT ON COLUMN show_state.theme IS 'Cosmetic theme identifier (e.g. jackpot_ps5 = NON-PAYOUT hype look). Not a money mechanic.';
