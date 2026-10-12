-- 001_init.sql — core schema. Postgres is the queue, the lock manager and the source of truth.

CREATE TABLE organizations (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    -- Fair-share weight: under contention an org with weight 2 is picked ~2x as often as weight 1.
    weight          INT  NOT NULL DEFAULT 1  CHECK (weight > 0),
    -- Fleet-wide cap on this org's concurrently running jobs (noisy-neighbour isolation).
    max_concurrency INT  NOT NULL DEFAULT 10 CHECK (max_concurrency > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 'failed' is not a state: a failed attempt goes back to 'scheduled' (with backoff) or to 'dead'.
CREATE TYPE job_status AS ENUM ('scheduled', 'running', 'succeeded', 'dead');

CREATE TABLE jobs (
    id               BIGSERIAL PRIMARY KEY,
    org_id           TEXT NOT NULL REFERENCES organizations(id),
    job_type         TEXT NOT NULL,
    -- 'slow' jobs may only occupy a bounded share of each consumer's worker slots.
    lane             TEXT NOT NULL DEFAULT 'fast' CHECK (lane IN ('fast', 'slow')),
    payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    idempotency_key  TEXT,
    scheduled_at     TIMESTAMPTZ NOT NULL,   -- what the client asked for; never changes
    run_at           TIMESTAMPTZ NOT NULL,   -- next eligible time (= scheduled_at, or retry backoff)
    status           job_status NOT NULL DEFAULT 'scheduled',
    attempts         INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 5   CHECK (max_attempts > 0),
    timeout_s        INT NOT NULL DEFAULT 300 CHECK (timeout_s > 0),
    -- Lease = (token, expiry). The token is a fencing token: it is regenerated on every claim, and
    -- every state transition out of 'running' must present it. A worker whose lease was reaped
    -- and re-granted to someone else can therefore never commit.
    lease_token      UUID,
    lease_owner      TEXT,
    lease_expires_at TIMESTAMPTZ,
    first_started_at TIMESTAMPTZ,            -- first pickup; schedule drift = first_started_at - scheduled_at
    started_at       TIMESTAMPTZ,            -- latest pickup
    finished_at      TIMESTAMPTZ,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT jobs_lease_iff_running CHECK ((status = 'running') = (lease_token IS NOT NULL)),
    -- Producer-side dedupe: re-submitting the same (org, key) is a no-op.
    CONSTRAINT jobs_idempotency UNIQUE (org_id, idempotency_key)
);

-- Claim path: per org + lane, oldest-eligible first. Partial, so it only indexes the pending set
-- and stays small no matter how much history accumulates.
CREATE INDEX jobs_claim_idx    ON jobs (org_id, lane, run_at, id) WHERE status = 'scheduled';
-- "When is the next job due?" — lets idle consumers sleep exactly until then.
CREATE INDEX jobs_next_due_idx ON jobs (run_at) WHERE status = 'scheduled';
-- Reaper (expired leases) and per-org running counts (fair-share + concurrency caps).
CREATE INDEX jobs_lease_idx    ON jobs (lease_expires_at) WHERE status = 'running';
CREATE INDEX jobs_running_idx  ON jobs (org_id) WHERE status = 'running';

-- One row per claim: the audit trail of every attempt and how it ended.
CREATE TABLE job_attempts (
    id          BIGSERIAL PRIMARY KEY,
    job_id      BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt     INT NOT NULL,
    lease_token UUID NOT NULL UNIQUE,
    consumer_id TEXT NOT NULL,
    claimed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    -- succeeded | failed | timeout | lease_expired (reaped) | fenced (late result rejected)
    outcome     TEXT,
    error       TEXT
);
CREATE INDEX job_attempts_job_idx ON job_attempts (job_id);

-- The job's side effect, committed in the SAME transaction that marks the job succeeded (and only
-- if the fencing token still matches). Deliberately NOT unique on job_id: the demo's "zero
-- duplicates" check is then a real test of the fencing logic rather than of a constraint. In
-- production you would add UNIQUE (job_id) as belt-and-braces.
CREATE TABLE job_effects (
    id           BIGSERIAL PRIMARY KEY,
    job_id       BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    lease_token  UUID NOT NULL,
    consumer_id  TEXT NOT NULL,
    result       JSONB,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX job_effects_job_idx ON job_effects (job_id);

-- Consumer registry (observability only — correctness never depends on it; leases do).
CREATE TABLE consumers (
    id                TEXT PRIMARY KEY,
    hostname          TEXT NOT NULL,
    pid               INT  NOT NULL,
    slots             INT  NOT NULL,
    inflight          INT  NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at        TIMESTAMPTZ
);

-- Wake idle consumers as soon as something becomes due "now" instead of waiting for the next poll.
-- Statement-level + constant payload: Postgres collapses duplicate notifications within a
-- transaction, so a 10k-row bulk insert sends one NOTIFY, not 10k.
CREATE FUNCTION notify_jobs_due() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM new_rows WHERE run_at <= now() + interval '1 second') THEN
        PERFORM pg_notify('jobs_due', '');
    END IF;
    RETURN NULL;
END $$;

CREATE TRIGGER jobs_notify_due
    AFTER INSERT ON jobs
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION notify_jobs_due();
