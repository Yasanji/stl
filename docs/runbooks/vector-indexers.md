# Vector — indexers runbook

Owner: vector team · Source rules: [alerts/vector-indexers.yaml](../../alerts/vector-indexers.yaml)

The Vector indexers (`stl-morpho-indexer`, `stl-oracle-indexer`) are the last
hop in the pipeline before customer-facing data lands in TimescaleDB. They
consume blocks from the watcher and derive product state (positions,
prices). A stall here directly translates to stale downstream reads.

---

## VectorMorphoIndexerStalled / VectorOracleIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

The labelled indexer on the labelled `chain` has processed zero blocks for
15 minutes. Downstream TimescaleDB state for Morpho positions / oracle
prices is going stale.

### First checks (≤5 min)

1. **Pod status** — run the one matching the firing alert:
   - Morpho: `kubectl -n vector get pods -l app=stl-morpho-indexer`
   - Oracle: `kubectl -n vector get pods -l app=stl-oracle-indexer`
2. **Recent logs** — look for decode panics, DB connection errors, or
   `context deadline exceeded` against the watcher's archive RPC.
3. **Upstream lag** — confirm the watcher is producing for this chain (if
   not, fix that first — that's `VectorWatcherNoBlocks`).
4. **TimescaleDB health** — connection pool exhaustion or replication lag
   can stall writes; check the Postgres dashboard.

### Common causes

- Indexer stuck on a malformed event after a contract upgrade → add the new
  ABI / decoder and redeploy.
- DB connection pool saturated → restart the pod; longer-term raise the
  pool limit.
- Watcher archive RPC slow → see `VectorMorphoIndexerRPCLatencyHigh`.

### Verify recovery

`rate({morpho,oracle}_blocks_processed_total) > 0` for the affected chain.

---

## VectorMorphoIndexerErrorsHigh / VectorOracleIndexerErrorsHigh

**Severity:** warning · **For:** 15m

### What it means

The labelled indexer is logging more than 0.1 errors/sec sustained for
15 minutes. Investigate before it escalates to a full stall.

### First checks

- Inspect recent logs for the dominant error class.
- Correlate with recent deploys (`kubectl rollout history`).
- Check for chain reorgs in the watcher logs — indexers may need to roll
  back state.

---

## VectorMorphoIndexerRPCLatencyHigh / VectorOracleIndexerRPCLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

p99 RPC request latency from the indexer (to the watcher's archive RPC) is
over 3 seconds sustained for 15 minutes. The indexer is degraded; expect
downstream lag.

### First checks

- Watcher RPC pod health and CPU usage.
- Network latency between the indexer node group and the watcher service.
- Recent changes in batch size / pagination defaults.

---

## maple-graphql-indexer (VEC-320)

Unlike morpho/oracle, `maple-graphql-indexer` is **not** a block consumer. It
is a Temporal cron (default 10m, `MAPLE_SYNC_INTERVAL`) that snapshots the
public Maple GraphQL API (`api.maple.finance/v2/graphql`) into TimescaleDB:
pools, active open-term loans + collateral, Sky strategies, Syrup globals.
There are no blocks and no RPC — the upstream is the Maple API, and "stale"
means the borrower-risk snapshot tables stop advancing. Single replica;
mainnet-only (`chain="ethereum"`).

Phases run per cycle, each in its own transaction: `pools` → `loans` →
`sky_strategies` → `syrup_globals`. A failed `pools` phase skips `loans` and
`sky_strategies` (they depend on the pool registry).

---

## VectorMapleIndexerStalled

**Severity:** critical · **For:** 15m

### What it means

No sync cycle has completed for >20m (two missed 10m intervals).
`maple_sync_cycles_total` (incremented on both success and error) is flat, so
the Maple snapshot tables are going stale.

### First checks (≤5 min)

1. **Is `VectorMapleIndexerDown` also firing?**
   - **Yes** → the pod is not running; this is a process death. Go to that
     runbook section — it's the root cause.
   - **No** → the pod is alive but not producing. Two likely causes:
     **(a)** the cron worker is wedged, or **(b)** the OTLP/metrics pipeline
     broke and the indexer is actually fine (this shape false-fired in
     VEC-360). Disambiguate with the next two steps.
2. **Pod + logs** — `kubectl -n vector logs -l app=maple-graphql-indexer --tail=100`.
   Healthy cycles log `pools synced` / `loans synced` / `syrup globals synced`
   every ~10m. If those lines are recent, the indexer is fine and the metrics
   pipeline is the problem (check the OTel collector in `observability`).
3. **Temporal** — confirm the schedule is firing: workflow IDs
   `scheduled-maple-graphql-indexer-<ts>` should appear each interval. If the
   worker lost its Temporal connection, restart the pod.
4. **Upstream** — `curl -s -XPOST https://api.maple.finance/v2/graphql` reachable?
   A hard Maple outage stalls every cycle (cycles still *run*, so this presents
   as cycle/phase errors too).

### Common causes

- OTLP export broken, indexer healthy (VEC-360) → fix the metrics pipeline, not
  the cron; logs are the source of truth.
- Temporal worker wedged / lost connection → restart the pod.
- Sustained Maple API outage → cycles fail; see `VectorMaplePhaseErrors`.

### Verify recovery

`rate(maple_sync_cycles_total[20m]) > 0`, and a fresh `synced_at` lands:
`SELECT max(synced_at) FROM maple_pool_state;` within the last interval.

---

## VectorMapleIndexerDown

**Severity:** critical · **For:** 10m

### What it means

The `maple-graphql-indexer` Deployment has <1 available replica for >10m
(kube-state-metrics, independent of the indexer's own OTLP export). The cron
worker is not running; no snapshots are taken. If `VectorMapleIndexerStalled`
is firing too, **this is the root cause.**

### First checks (≤5 min)

1. `kubectl -n vector get deploy,pods -l app=maple-graphql-indexer` — pod state
   and restart count.
2. **Not Running?** describe for the reason:
   `kubectl -n vector describe pod -l app=maple-graphql-indexer | sed -n '/Events:/,$p'`
   - `CreateContainerConfigError` / `CreateContainerError` → missing config or
     secret; check the `maple-graphql-indexer` ConfigMap and ExternalSecret.
   - `CrashLoopBackOff` → read logs; common: bad `DATABASE_URL`, unreachable
     TimescaleDB, or a startup panic.
   - `Pending` / `FailedScheduling` → no node capacity; check the node group.
3. **ExternalSecret synced?**
   `kubectl -n vector get externalsecret maple-graphql-indexer` — a failed sync
   leaves the pod unable to start.

### Common causes

- Missing/failed ExternalSecret (DB creds) → fix the secret, pod recovers.
- Crash on startup (DB unreachable, config) → fix the dependency.
- Node scaled away with no rescheduling capacity → check the node group /
  autoscaler.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="maple-graphql-indexer"} == 1`
and cycles resume (see Stalled recovery check).

---

## VectorMapleIndexerCycleErrors / VectorMaplePhaseErrors

**Severity:** warning · **For:** 0m (the 30m window debounces)

### What it means

Cycles are running but failing. `CycleErrors` is the rollup (also catches
pre-phase setup failures: protocol seed, DB connection); `PhaseErrors` names
the failing `phase` and is the actionable one. Per-phase transactions mean
surviving phases still commit — data is **partial, not absent**, so this warns
rather than pages.

### First checks

1. **Which phase** — `sum by (phase) (increase(maple_sync_phases_total{status="error"}[30m]))`.
2. **Logs** for that phase — `kubectl -n vector logs -l app=maple-graphql-indexer`.
   Errors name the owning entity (e.g. `pool 0x… : …`).
3. **Cascade check** — a failing `pools` phase deliberately skips `loans` and
   `sky_strategies`; fix `pools` first.

### Common causes

- Maple API returning Apollo `errors[]`, 429/5xx, or unstable pagination
  (duplicate IDs across pages) → usually transient; confirm it clears next cycle.
- A string-encoded integer field the client can't parse → the phase fails hard
  by design (never silently skips rows); needs a client fix.
- DB write failure (FK, constraint) → inspect the named entity.

---

## VectorMaplePoolWritesZero

**Severity:** warning · **For:** 15m

### What it means

Cycles are running but the `pools` phase wrote 0 rows over 20m. There are
always ~21 PoolV2 pools, so 0 means Maple's `poolV2S` query returned an empty
(but HTTP 200, no-error) collection — a silent upstream regression the error
alerts do **not** catch. (This is gated on "cycling AND zero" so it doesn't
just duplicate `Stalled`.)

### First checks

- Query the API directly for `poolV2S` and confirm it returns pools.
- Check `maple-graphql-client` logs for `fetched paginated collection
  collection=pools total_rows=0`.
- If Maple changed the query shape/field names, the client needs updating.

> Note: do **not** expect an equivalent for loans — the service legitimately
> records 0 loan rows when there are no active loans, so loan-zero is normal.

---

## VectorMapleSchemaDrift

**Severity:** warning · **For:** 0m (1h window debounces)

### What it means

A field Maple normally populates (`pool_monthly_apy`, `pool_spot_apy`,
`strategy_fee_rate`, `strategy_total_fees_collected`) was null-downgraded to
SQL NULL repeatedly (>5/1h). Known-nullable fields (`loan_acm_ratio`,
`pool_tvl`, `pool_collateral_value_usd`, `syrup_drips_yield_boost`) are
excluded. A sustained count signals a Maple GraphQL API schema change, not a
code bug.

### First checks

- `sum by (field) (increase(maple_sync_null_downgrades_total[1h]))` — which
  field(s).
- Cross-check the field against Maple's published SDL / docs.
- Inspect rows: `SELECT * FROM maple_pool_state WHERE <field> IS NULL ORDER BY
  synced_at DESC LIMIT 20;` — confirm the API, not our parsing, dropped it.

### Action

If Maple intentionally made the field nullable, move it to the excluded set and
re-baseline (ORB-145). If it's an upstream regression, raise with Maple and
decide whether downstream consumers tolerate the gap. The allowlist and the
`>5/1h` threshold are pre-baseline estimates — tune off a few days of data.

---

## VectorMaplePhaseLatencyHigh

**Severity:** warning · **For:** 15m

### What it means

A phase p95 duration exceeded 30s over 15m. Normal p95 is seconds (loans ~4s,
others <1s) and is essentially Maple API response time — there is no RPC here.
30s is generous for ~150 rows/cycle; sustained latency this high means the
Maple API is degraded and a phase risks overrunning the 10m interval.

### First checks

- `histogram_quantile(0.95, sum by (phase, le) (rate(maple_sync_phase_duration_seconds_bucket[15m])))`
  — which phase.
- Maple API status / response times (curl a representative query).
- If it's the `loans` phase, pagination volume may have grown; check
  `total_rows` in the client logs.

---

## fluid-vault-indexer (VEC-438)

`fluid-vault-indexer` consumes Ethereum BlockEvents, reads end-of-block Fluid
(Instadapp) vault state from the VaultResolver via Multicall3, and appends
`fluid_vault_state` snapshots into TimescaleDB. Mainnet (chain 1) only.

**Metric coverage:** the service emits **no service-level counters/histograms of
its own** today. These alerts use the signals that genuinely exist:
`kube_deployment_status_replicas_available` (process liveness, independent of the
OTel pipeline), `multicall_batch_size_count{service_name="fluid-vault-indexer"}`
(advances on startup reconcile + every block touching a known vault), and the
shared `VectorArchiving*` rules (raw-SC-call archive health, keyed by
`service_name`). Error-rate, silent-empty (rows-written == 0), and RPC-latency
alerts are intentionally absent — no metric would make them fire honestly, so
they are omitted rather than shipped as rules that can never fire. Adding
`fluid_blocks_processed_total` / `fluid_errors_total` / a rows-written counter /
an RPC-latency histogram to the B2 service is a follow-up instrumentation task;
grow the rules + these sections when those land.

---

## VectorFluidVaultIndexerDown

**Severity:** critical · **For:** 10m

### What it means

The `fluid-vault-indexer` Deployment has <1 available replica for 10 minutes. No
pod is running, so no Fluid vault snapshots are written and the SQS backlog is
growing. This is the keystone freshness signal for a service with no internal
block-progress metric.

### First checks (≤5 min)

1. **Pod status** — `kubectl -n vector get pods -l app=fluid-vault-indexer`.
2. **Why it's not ready** — `kubectl -n vector describe deployment/fluid-vault-indexer`
   and `kubectl -n vector logs -l app=fluid-vault-indexer --previous` for a crash
   loop (missing queue URL, DB/Redis/RPC dial failure, bad ABI load).
3. **Secrets/config present** — the worker requires `AWS_SQS_QUEUE_URL`,
   `DATABASE_URL`, `ALCHEMY_API_KEY`, `REDIS_ADDR`, `S3_BUCKET`. A missing key
   from the `fluid-vault-indexer` ExternalSecret crashes it on startup. The queue
   URL is the `ethereum_sqs_fluid_vault_url` property of `stl-<env>-infra-config`
   (created by infra VEC-439).
4. **Node/scheduling** — pending pod => check node capacity / taints.

### Common causes

- ExternalSecret not yet synced (queue URL / DB URL missing) — the deploy ran
  before the infra apply. Verify VEC-439 applied; re-sync the ExternalSecret.
- Crash loop on a startup error (DB/Redis/RPC unreachable) — fix the dependency;
  the worker is fail-fast by design.
- OOMKilled — check memory limits.

### Verify recovery

`kube_deployment_status_replicas_available{deployment="fluid-vault-indexer"} >= 1`
and the pod logs show `fluid vault indexer started, waiting for messages...`.

---

## VectorFluidVaultIndexerStalled

**Severity:** warning · **For:** 15m

### What it means

The worker is **up** (≥1 replica) but has executed **no VaultResolver multicalls
for 30 minutes**. A healthy worker on mainnet advances
`multicall_batch_size_count` regularly (startup reconcile + every block touching
a known vault), so a sustained zero means one of: a benign quiet period (no
in-scope sUSDS-debt vault touched, no restart), a wedged poll loop (process alive
but not processing), or a broken OTLP export (worker fine, metrics stopped — in
which case `VectorFluidVaultIndexerDown` is **not** firing and other OTel series
from the pod also go flat).

### First checks (≤5 min)

1. **Distinguish the cases** — is `VectorFluidVaultIndexerDown` also firing? If
   so the process is down (treat as Down). If not, the pod is alive — suspect a
   wedged loop or a metrics-pipeline issue.
2. **Recent logs** — `kubectl -n vector logs -l app=fluid-vault-indexer --tail=200`.
   Look for a repeating error on one message, `context deadline exceeded` against
   the Alchemy RPC, or silence (poll loop stopped).
3. **SQS backlog** — check the `stl-<env>-ethereum-fluid_vault.fifo` queue depth.
   Growing `ApproximateNumberOfMessages` + zero multicalls = wedged (not
   draining); near-empty queue + zero multicalls = genuine quiet period (benign).
4. **Upstream** — confirm the ethereum watcher is still producing blocks; if not,
   that's the root cause (`VectorWatcherNoBlocks`).

### Common causes

- Poison message wedging the poll loop — inspect the DLQ; redrive or purge the
  offending message.
- Alchemy RPC degraded / rate-limited — multicalls time out; check logs and the
  Alchemy status.
- Genuine quiet period on a low-activity target debt token — no action; clears
  once a vault is next touched.

### Verify recovery

`increase(multicall_batch_size_count{service_name="fluid-vault-indexer"}[30m]) > 0`,
or confirm the SQS backlog is draining and recent logs show snapshots written.

---

## See also

- Watcher runbook: [vector-watcher.md](vector-watcher.md)
- Backup worker runbook: [vector-backup-worker.md](vector-backup-worker.md)
