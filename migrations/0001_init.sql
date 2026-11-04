-- API keys: only a SHA-256 of the key is stored (design §4).
CREATE TABLE api_keys (
  id          uuid PRIMARY KEY,
  name        text NOT NULL,
  key_prefix  text NOT NULL,
  key_hash    bytea NOT NULL UNIQUE,
  rpm_limit   integer CHECK (rpm_limit > 0),   -- NULL = unlimited
  tpm_limit   integer CHECK (tpm_limit > 0),   -- NULL = unlimited
  created_at  timestamptz NOT NULL DEFAULT now(),
  revoked_at  timestamptz
);

-- Materialized per-key totals; must always equal what ledger_entries derives (design §8.4).
CREATE TABLE key_accounts (
  key_id      uuid PRIMARY KEY REFERENCES api_keys (id),
  budget_nusd bigint CHECK (budget_nusd >= 0), -- NULL = unlimited; a spend cap, not a balance
  spent_nusd  bigint NOT NULL DEFAULT 0,       -- Σ settle
  held_nusd   bigint NOT NULL DEFAULT 0,       -- Σ reserve of requests with no release
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TYPE ledger_kind AS ENUM ('reserve', 'settle', 'release');

CREATE TABLE ledger_entries (
  id                bigserial PRIMARY KEY,
  request_id        uuid NOT NULL,
  key_id            uuid NOT NULL REFERENCES api_keys (id),
  kind              ledger_kind NOT NULL,
  amount_nusd       bigint NOT NULL CHECK (amount_nusd >= 0),
  alias             text,
  provider          text,
  upstream_model    text,
  prompt_tokens     integer,
  completion_tokens integer,
  usage_estimated   boolean,
  source            text NOT NULL CHECK (source IN ('gateway', 'reconciler')),
  reason            text,
  meta              jsonb,
  created_at        timestamptz NOT NULL DEFAULT now(),
  -- The idempotency key. Every closing path inserts a 'release', so this also
  -- guarantees each request is closed exactly once (design §8.4).
  UNIQUE (request_id, kind)
);

CREATE INDEX ledger_entries_key_created_idx ON ledger_entries (key_id, created_at);
CREATE INDEX ledger_entries_reserve_created_idx ON ledger_entries (created_at) WHERE kind = 'reserve';

CREATE FUNCTION ledger_entries_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'ledger_entries is append-only: % is not allowed', TG_OP;
END
$$;

CREATE TRIGGER ledger_entries_no_update_delete
  BEFORE UPDATE OR DELETE ON ledger_entries
  FOR EACH ROW EXECUTE FUNCTION ledger_entries_append_only();

CREATE TRIGGER ledger_entries_no_truncate
  BEFORE TRUNCATE ON ledger_entries
  FOR EACH STATEMENT EXECUTE FUNCTION ledger_entries_append_only();
