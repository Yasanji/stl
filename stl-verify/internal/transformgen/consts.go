package transformgen

// This file is the static (non-per-table) SQL of the bucket-1 migration, embedded
// verbatim from db/migrations/20260706_140000_create_transformed_bucket1.sql: the
// header (schema + _sources registry) and the tail (queue-status view, parity
// ledger machinery, grants, migration-tracking insert). These do not vary per
// table, so the generator emits them unchanged; the regen-diff test compares them
// (normalised) against the committed migration.

const header = `-- Transformation layer, bucket 1 (VEC-484): 13 governed tables canonicalised 1:1 from raw
-- (rename / cast / dimension-fill), each a hypertable with a PK derived from the raw PK.
--
-- Refresh model: a trigger-maintained change queue (replaces the build_id watermark, which was
-- unsound: build_id identifies a git build, not a commit position, so it is reused on rollback
-- and differs across the multiple binaries that write one raw table; a build_id >= watermark
-- cursor silently drops rows from a concurrent lower-build writer, a rollback, or an older-image
-- backfill. Instead:
--   * Each raw table gets an AFTER INSERT row trigger that appends the new row's raw PK to a
--     per-table queue transformed._pending_<t>. The queue row commits in the same transaction as
--     the raw row, so overlap / rollback / backfill / multi-writer are all non-issues, and nothing
--     that inserts into raw can bypass it. Raw is append-only (corrections arrive as a new
--     processing_version row -> a new PK), so an INSERT trigger is sufficient.
--   * transformed._run_<t>() drains its queue in bounded batches (DELETE ... RETURNING under an
--     advisory lock), re-reads just those raw rows by PK (recent, uncompressed, untiered -- no
--     full scan, no S3 read), applies the transform, and upserts ON CONFLICT DO UPDATE ... WHERE
--     IS DISTINCT FROM. The upsert keeps the layer re-runnable -- a re-enqueue or a re-bootstrap
--     after a transform-logic fix or a late-arriving dimension refreshes the row -- while the
--     IS DISTINCT FROM guard skips the write when nothing changed, so there is no churn. It
--     returns the queue rows consumed; the worker loops until the queue is drained.
--   * The trigger functions are SECURITY DEFINER (owned by the migration role) so enqueue never
--     depends on the raw writer holding a grant on the queue -- a grant gap can never halt ingest.
--   * Bootstrap of pre-existing history is out-of-band (transformed._bootstrap_<t>(_from,_to) run
--     in time windows by a one-off job with tiered reads enabled), NOT through the 10-minute
--     Temporal activity. The trigger is live before bootstrap starts, so the overlap window is
--     closed by the upsert's idempotency (re-inserting an identical row is a guarded no-op).
--
-- Idempotent / non-destructive: schema, tables, queues, triggers and functions are all created
-- IF NOT EXISTS / OR REPLACE and PK ALTERs are guarded, so re-applying is a no-op.
--
-- COMMENT ON metadata follows the catalogue convention (see 20260609_120000_add_schema_comments.sql).
-- Comment text is carried verbatim from each raw source table; a raw column that was itself
-- undocumented is flagged "verify before computing" rather than given a fabricated unit/scale.

CREATE SCHEMA IF NOT EXISTS transformed;

-- Registry of transformed tables the worker iterates (replaces the _watermark row set).
CREATE TABLE IF NOT EXISTS transformed._sources(source text PRIMARY KEY);
COMMENT ON TABLE transformed._sources IS
  '[Operational] One row per transformed table. The transform worker lists these and calls transformed._run_<source>() for each.';
COMMENT ON COLUMN transformed._sources.source IS 'PK. Transformed table name whose _run_<source>()/_bootstrap_<source>() functions and _pending_<source> queue exist.';


-- ===== morpho_market_state =====
`

const tail = `CREATE OR REPLACE VIEW transformed._queue_status AS
SELECT 'morpho_market_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_market_state"
UNION ALL
SELECT 'morpho_market_position'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_market_position"
UNION ALL
SELECT 'morpho_vault_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_vault_state"
UNION ALL
SELECT 'morpho_vault_position'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_vault_position"
UNION ALL
SELECT 'fluid_vault_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_fluid_vault_state"
UNION ALL
SELECT 'token_total_supply'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_token_total_supply"
UNION ALL
SELECT 'onchain_token_price'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_onchain_token_price"
UNION ALL
SELECT 'maple_loan_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_loan_state"
UNION ALL
SELECT 'maple_loan_collateral'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_loan_collateral"
UNION ALL
SELECT 'maple_pool_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_pool_state"
UNION ALL
SELECT 'maple_sky_strategy_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_sky_strategy_state"
UNION ALL
SELECT 'maple_syrup_global_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_syrup_global_state"
UNION ALL
SELECT 'offchain_token_price'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_offchain_token_price";
COMMENT ON VIEW transformed._queue_status IS '[Operational] Per-source pending-row count and oldest enqueue time across the transform change queues. oldest_enqueued_at lagging wall-clock = a stalled transform.';

-- Raw-vs-transformed parity backstop (checkpointed incremental).
-- A ledger holds a verified (raw, transformed, pending) count per source per
-- raw-chunk time-range plus that chunk's pg_stat_all_tables activity baseline, so
-- the worker does not full-count every table each tick. _parity_refresh re-counts
-- only the head chunk and any local chunk whose activity moved (an insert signal
-- the enqueue trigger does NOT own, so a broken trigger still surfaces) or reset;
-- tiered chunks are verified once at bootstrap (_parity_verify_all, tiered reads
-- on) and frozen. drift = sum(raw - transformed - pending) over the ledger; 0 in a
-- consistent snapshot, nonzero = a row that reached neither the transformed table
-- nor its queue.
CREATE TABLE IF NOT EXISTS transformed._parity_ledger(
  source        text        NOT NULL,
  bucket_start  timestamptz NOT NULL,
  bucket_end    timestamptz NOT NULL,
  raw_count     bigint      NOT NULL,
  xform_count   bigint      NOT NULL,
  pending_count bigint      NOT NULL,
  activity      bigint,
  frozen        boolean     NOT NULL DEFAULT false,
  verified_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, bucket_start)
);
COMMENT ON TABLE transformed._parity_ledger IS '[Operational] Checkpointed parity: verified raw/transformed/pending counts per source per raw-chunk time-range, with the chunk pg_stat activity baseline that decides when to re-verify. Tiered buckets are frozen (verified once at bootstrap).';
COMMENT ON COLUMN transformed._parity_ledger.source IS 'PK. Transformed table name this bucket belongs to.';
COMMENT ON COLUMN transformed._parity_ledger.bucket_start IS 'PK. Start of the raw-chunk time-range this bucket covers (a time boundary, so raw and transformed -- which have different chunk intervals -- are compared over the same window).';
COMMENT ON COLUMN transformed._parity_ledger.activity IS 'Raw chunk n_tup_ins+n_tup_del at last verify; a change/reset re-verifies the bucket. NULL for frozen (tiered) buckets.';
COMMENT ON COLUMN transformed._parity_ledger.frozen IS 'Tiered bucket verified once at bootstrap; never re-counted by the per-tick refresh (would re-read S3).';

-- _parity_count: count rows of _tbl whose time column _col is in [_lo, _hi).
CREATE OR REPLACE FUNCTION transformed._parity_count(_tbl text, _col text, _lo timestamptz, _hi timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  EXECUTE format('SELECT count(*) FROM %s WHERE %I >= $1 AND %I < $2', _tbl, _col, _col) INTO n USING _lo, _hi;
  RETURN n;
END $fn$ LANGUAGE plpgsql;

-- _parity_refresh: steady-state incremental refresh for one source. Iterates only
-- LOCAL chunks (never references the tiered/OSM catalog, so it is safe where OSM is
-- absent, e.g. the OSS test image). Re-counts the head chunk plus any chunk whose
-- activity changed, reset, or is new; skips unchanged non-head local buckets.
CREATE OR REPLACE FUNCTION transformed._parity_refresh(_source text) RETURNS void AS $fn$
DECLARE
  raw_col text; xf_col text; head_start timestamptz;
  raw_tbl  text := format('public.%I', _source);
  xf_tbl   text := format('transformed.%I', _source);
  pend_tbl text := format('transformed.%I', '_pending_' || _source);
  c record;
BEGIN
  SELECT column_name INTO raw_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='public' AND hypertable_name=_source;
  SELECT column_name INTO xf_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='transformed' AND hypertable_name=_source;
  IF raw_col IS NULL OR xf_col IS NULL THEN
    RAISE EXCEPTION 'parity_refresh: no time dimension for %', _source;
  END IF;

  SELECT max(range_start) INTO head_start FROM timescaledb_information.chunks
    WHERE hypertable_schema='public' AND hypertable_name=_source;

  FOR c IN
    SELECT ch.range_start AS rs, ch.range_end AS re,
           COALESCE(st.n_tup_ins,0) + COALESCE(st.n_tup_del,0) AS activity
    FROM timescaledb_information.chunks ch
    LEFT JOIN pg_stat_all_tables st
      ON st.schemaname = ch.chunk_schema AND st.relname = ch.chunk_name
    WHERE ch.hypertable_schema='public' AND ch.hypertable_name=_source
  LOOP
    IF c.rs IS DISTINCT FROM head_start
       AND EXISTS (SELECT 1 FROM transformed._parity_ledger l
                   WHERE l.source=_source AND l.bucket_start=c.rs
                     AND NOT l.frozen AND l.activity = c.activity)
    THEN
      CONTINUE;  -- unchanged non-head local bucket
    END IF;
    INSERT INTO transformed._parity_ledger
      (source, bucket_start, bucket_end, raw_count, xform_count, pending_count, activity, frozen, verified_at)
    VALUES (_source, c.rs, c.re,
            transformed._parity_count(raw_tbl,  raw_col, c.rs, c.re),
            transformed._parity_count(xf_tbl,   xf_col,  c.rs, c.re),
            transformed._parity_count(pend_tbl, raw_col, c.rs, c.re),
            c.activity, false, now())
    ON CONFLICT (source, bucket_start) DO UPDATE SET
      bucket_end=EXCLUDED.bucket_end, raw_count=EXCLUDED.raw_count,
      xform_count=EXCLUDED.xform_count, pending_count=EXCLUDED.pending_count,
      activity=EXCLUDED.activity, frozen=false, verified_at=now();
  END LOOP;
END $fn$ LANGUAGE plpgsql;

-- _parity_verify_all: full verify for one source, for bootstrap (run with tiered
-- reads ON). Re-counts every local chunk and every tiered chunk, freezing tiered
-- buckets so the per-tick refresh never re-reads S3. The tiered/OSM catalog is
-- referenced only through a guarded dynamic query, so the tiered pass is a no-op
-- where OSM is absent.
CREATE OR REPLACE FUNCTION transformed._parity_verify_all(_source text) RETURNS void AS $fn$
DECLARE
  raw_col text; xf_col text;
  raw_tbl  text := format('public.%I', _source);
  xf_tbl   text := format('transformed.%I', _source);
  pend_tbl text := format('transformed.%I', '_pending_' || _source);
  c record;
BEGIN
  SELECT column_name INTO raw_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='public' AND hypertable_name=_source;
  SELECT column_name INTO xf_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='transformed' AND hypertable_name=_source;
  IF raw_col IS NULL OR xf_col IS NULL THEN
    RAISE EXCEPTION 'parity_verify_all: no time dimension for %', _source;
  END IF;

  FOR c IN
    SELECT ch.range_start AS rs, ch.range_end AS re,
           COALESCE(st.n_tup_ins,0) + COALESCE(st.n_tup_del,0) AS activity
    FROM timescaledb_information.chunks ch
    LEFT JOIN pg_stat_all_tables st
      ON st.schemaname = ch.chunk_schema AND st.relname = ch.chunk_name
    WHERE ch.hypertable_schema='public' AND ch.hypertable_name=_source
  LOOP
    INSERT INTO transformed._parity_ledger
      (source, bucket_start, bucket_end, raw_count, xform_count, pending_count, activity, frozen, verified_at)
    VALUES (_source, c.rs, c.re,
            transformed._parity_count(raw_tbl,  raw_col, c.rs, c.re),
            transformed._parity_count(xf_tbl,   xf_col,  c.rs, c.re),
            transformed._parity_count(pend_tbl, raw_col, c.rs, c.re),
            c.activity, false, now())
    ON CONFLICT (source, bucket_start) DO UPDATE SET
      bucket_end=EXCLUDED.bucket_end, raw_count=EXCLUDED.raw_count,
      xform_count=EXCLUDED.xform_count, pending_count=EXCLUDED.pending_count,
      activity=EXCLUDED.activity, frozen=false, verified_at=now();
  END LOOP;

  IF to_regclass('timescaledb_osm.tiered_chunks') IS NOT NULL THEN
    FOR c IN EXECUTE format(
      'SELECT range_start AS rs, range_end AS re FROM timescaledb_osm.tiered_chunks WHERE hypertable_schema=%L AND hypertable_name=%L',
      'public', _source)
    LOOP
      INSERT INTO transformed._parity_ledger
        (source, bucket_start, bucket_end, raw_count, xform_count, pending_count, activity, frozen, verified_at)
      VALUES (_source, c.rs, c.re,
              transformed._parity_count(raw_tbl,  raw_col, c.rs, c.re),
              transformed._parity_count(xf_tbl,   xf_col,  c.rs, c.re),
              transformed._parity_count(pend_tbl, raw_col, c.rs, c.re),
              NULL, true, now())
      ON CONFLICT (source, bucket_start) DO UPDATE SET
        bucket_end=EXCLUDED.bucket_end, raw_count=EXCLUDED.raw_count,
        xform_count=EXCLUDED.xform_count, pending_count=EXCLUDED.pending_count,
        activity=NULL, frozen=true, verified_at=now();
    END LOOP;
  END IF;
END $fn$ LANGUAGE plpgsql;

-- Per-source drift, summed from the ledger (cheap; no table scan).
CREATE OR REPLACE VIEW transformed._parity_status AS
SELECT source,
       sum(raw_count)     AS raw_rows,
       sum(xform_count)   AS transformed_rows,
       sum(pending_count) AS pending_rows,
       sum(raw_count - xform_count - pending_count) AS drift
FROM transformed._parity_ledger
GROUP BY source;
COMMENT ON VIEW transformed._parity_status IS '[Operational] Per-source raw vs transformed vs pending counts and drift (raw - transformed - pending), summed from transformed._parity_ledger. drift is 0 in a consistent snapshot after bootstrap; nonzero = a silent queue/trigger/bootstrap gap.';

-- Grants: the worker connects as stl_readwrite and drains the queues via _run_<t>()
-- (SECURITY INVOKER), so it needs schema usage, table read/write, and EXECUTE. The enqueue
-- triggers are SECURITY DEFINER, so raw writers need no grant on the queue tables to insert.
-- stl_readonly gets read access for downstream consumers of the transformed layer.
GRANT USAGE ON SCHEMA transformed TO stl_readonly, stl_readwrite;
GRANT SELECT ON ALL TABLES IN SCHEMA transformed TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA transformed TO stl_readwrite;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA transformed TO stl_readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA transformed GRANT SELECT ON TABLES TO stl_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA transformed GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stl_readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA transformed GRANT EXECUTE ON FUNCTIONS TO stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260706_140000_create_transformed_bucket1.sql')
ON CONFLICT (filename) DO NOTHING;
`
