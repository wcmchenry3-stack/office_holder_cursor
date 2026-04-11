# Operational Settings Reference

Operational constants that previously required a code change to adjust are now stored in the `app_settings` DB table and editable via the `/data/scheduled-jobs` UI. Some changes take effect immediately; others require a server restart.

---

## `app_settings` table

All 12 settings with their defaults:

| Key | Default | Live? | Description |
|---|---|---|---|
| `expiry_hours_queued` | `12` | ✅ Live | Hours before a queued scraper job is automatically expired |
| `expiry_hours_running_full` | `24` | ✅ Live | Hours before a running full-scrape job is expired |
| `expiry_hours_running_other` | `8` | ✅ Live | Hours before other running jobs (delta, vitals, etc.) are expired |
| `max_queued_jobs` | `1` | ✅ Live | Max jobs allowed to queue behind an active job (1 = only one job waits) |
| `cron_daily_maintenance_hour` | `5` | ❌ Restart | UTC hour for the daily maintenance job |
| `cron_daily_maintenance_minute` | `30` | ❌ Restart | UTC minute for the daily maintenance job |
| `cron_daily_delta_hour` | `6` | ❌ Restart | UTC hour for the daily delta scrape |
| `cron_daily_delta_minute` | `0` | ❌ Restart | UTC minute for the daily delta scrape |
| `cron_daily_insufficient_vitals_hour` | `7` | ❌ Restart | UTC hour for the insufficient vitals job |
| `cron_daily_insufficient_vitals_minute` | `0` | ❌ Restart | UTC minute for the insufficient vitals job |
| `cron_daily_gemini_research_hour` | `8` | ❌ Restart | UTC hour for the Gemini research job |
| `cron_daily_gemini_research_minute` | `0` | ❌ Restart | UTC minute for the Gemini research job |
| `cron_daily_page_quality_hour` | `9` | ❌ Restart | UTC hour for the page quality inspection job |
| `cron_daily_page_quality_minute` | `0` | ❌ Restart | UTC minute for the page quality inspection job |

**"Live"** means the setting is read at the point of use — changes apply to the next job execution with no restart needed.

**"Restart required"** means the setting is read once at startup when APScheduler registers the cron jobs. A change written to the DB takes no effect until the server restarts.

### Fault tolerance

`get_setting(key, default)` is fault-tolerant: if the DB is unavailable or the key is missing, it returns the hardcoded default. The app never crashes due to a missing or inaccessible setting.

---

## `scheduler_settings` table (per-job pause)

Each scheduled job can be independently paused and resumed via the `/data/scheduled-jobs` UI. Pause state is stored in `scheduler_settings` and survives server restarts.

### How pausing works

APScheduler itself is never paused — the scheduler fires each job on schedule regardless. Each job entry point checks `is_job_paused(job_id)` at the very start and returns early if paused. This means:

- No DB writes occur during a paused run
- The next scheduled fire will also check and return early
- Resume takes effect on the next scheduled fire (no restart needed)

### Non-pauseable jobs

The `daily_maintenance` job is not pauseable — it always runs even when other jobs are paused. This ensures stale job expiry and disk cache cleanup always happen. It does not perform any scraping.

---

## `RUNNERS_ENABLED` — global kill switch

Setting the `RUNNERS_ENABLED` environment variable to `0`, `false`, `no`, or `off` disables all scheduled and manual runner jobs globally without a code deployment.

```
RUNNERS_ENABLED=0
```

This overrides all per-job pause states. The scheduled jobs still fire (APScheduler is not stopped) but each job entry point checks `is_runners_enabled()` and returns immediately if disabled.

**When to use:** During incident response, maintenance windows, or whenever you need to stop all scraping immediately. Change the env var on Render and restart the service.

**Relationship to per-job pause:** Per-job pause is for routine scheduling control (e.g. "pause gemini_research for a week"). `RUNNERS_ENABLED=0` is the emergency stop for all activity.

---

## UI pages

### `/data/scheduled-jobs`

The primary operational control surface. Shows all 5 scheduled jobs with:
- Current cron schedule (hour:minute UTC) — editable inline; changes require restart
- Pause/resume toggle per job
- Last run result, status, and duration
- **Expiry Thresholds & Queue** section — edit `expiry_hours_*` and `max_queued_jobs` live

### `/data/scheduled-job-runs`

Read-only history of all APScheduler job executions. Shows status (color-coded), duration, and a summary column:
- For scraper jobs: terms parsed, bio success/error counts
- For page quality jobs: result badge and attempt count

Filterable by time range: 30d / 90d / 365d.

### `/data/scraper-jobs`

History of all `scraper_jobs` table records — both manual UI-triggered jobs and queued jobs. Shows job type, status, queued/started timestamps, and result. Includes the **Force Expire Stale Jobs** button for emergency recovery.

---

## Job queue and stale job expiry

When a job is already running, new manual submissions are queued in the `scraper_jobs` table (up to `max_queued_jobs`, default 1). When the running job completes, `_maybe_start_next_queued_job()` automatically dequeues and starts the next one.

### `max_queued_jobs` — queue depth limit

`max_queued_jobs` (default `1`) is the maximum number of jobs that may wait behind an already-running job. When a submission arrives and the queue is already at this depth, the API returns `{"queued": false, "reason": "queue_full"}` with HTTP 202 — the job is **rejected, not dropped**. No existing queued job is removed. Raise this value (e.g. to `2`) if you need more buffering; lower it to `0` to prevent any queuing (every submission while a job is running is immediately rejected).

This setting is **Live** — changes in the UI take effect on the very next submission with no restart.

### How pause/resume interacts with the queue

Pausing a job type does **not** drain or cancel its queue. Queued `scraper_jobs` records remain in the table. When the job is resumed, the next submission or the next completion of a running job will dequeue normally. Pausing only prevents *new* scheduled or manual runs from starting a new thread — it does not affect jobs that are already queued or running.

### Queue visibility

- **`/data/scraper-jobs`** — lists all `scraper_jobs` records including queued ones. The status column shows `queued` vs. `running` vs. `complete`/`error`.
- **`GET /api/run/status`** — returns the in-memory job state for the currently running job.
- **`/data/scheduled-jobs` → Expiry Thresholds & Queue** — shows the current `max_queued_jobs` value and lets you edit it live.

### Stale job expiry on startup

On every server start (including restarts after OOM kills or Render redeploys), the lifespan hook calls `expire_stale_scheduled_job_runs(stale_hours=0)`. Using `stale_hours=0` expires **every** `scheduled_job_runs` row still in `running` state, regardless of age. This is intentional: any job that was in-flight when the previous process died cannot be running in the new process. Leaving those rows alive with the normal 4-hour threshold would block downstream scheduled jobs (`daily_insufficient_vitals`, `daily_gemini_research`, `daily_page_quality`) for hours after a restart. The expired rows are marked `error` with a reason string that includes "stale-run cleanup" so they are visible in `/data/scheduled-job-runs`.

### Nightly stale job expiry

The `daily_maintenance` job (05:30 UTC) independently expires stale `scraper_jobs` rows via `expire_stale_jobs()`:
- Queued jobs older than `expiry_hours_queued` (default 12h) are expired
- Running full-scrape jobs older than `expiry_hours_running_full` (default 24h) are expired
- Other running jobs older than `expiry_hours_running_other` (default 8h) are expired

Expiring a job also calls `_cancel_in_memory_job()` to stop the background thread, and sends an email notification per expired job.

For immediate recovery (e.g. a stuck job that won't expire until tomorrow's maintenance run), use `POST /api/run/force-expire-stale` — accessible via the button on `/data/scraper-jobs`.

---

## Linear scheduling guard

Each scheduled job (except `daily_maintenance`) calls `_has_active_scheduled_run(job_name)` **before** creating its `scheduled_job_runs` row. If any other `scheduled_job_runs` row has `status='running'` and `started_at` within the last 4 hours, the calling job logs a warning and returns early — it does not run.

### How the guard detects an in-progress run

The guard queries the `scheduled_job_runs` table via `count_active_scheduled_runs(active_hours=4)`. It counts rows where `status = 'running'` and `started_at >= now - 4h`. It does **not** use an in-memory lock, so the guard works correctly across restarts: a job started before a restart but still recorded as `running` in the DB will block the next fire of any other job until the startup expiry or `daily_maintenance` clears that row.

Because `create_run` is called **after** the guard check, a job cannot block itself through its own row — the row simply does not exist yet when the guard fires.

### Scope

All four pauseable scheduled jobs are guarded: `daily_delta`, `daily_insufficient_vitals`, `daily_gemini_research`, and `daily_page_quality`. `daily_maintenance` is **not** guarded — it always runs to ensure stale-job expiry and disk cache cleanup happen regardless of other job state.

### Observability

A skipped run logs at `WARNING` level:

```
daily_delta skipped: 1 other scheduled job(s) running (linear scheduling guard)
```

This message appears in the Render container log. The skipped invocation is **not** written to `scheduled_job_runs`, so it will not appear in `/data/scheduled-job-runs`. Only runs that actually start (past the guard) produce a DB record.

### Override

There is no built-in UI flag to force a run through the guard. If you need to run a job while another is still active:

1. Use `POST /api/run/force-expire-stale` (or the button on `/data/scraper-jobs`) to expire the blocking row, then trigger the job manually.
2. Alternatively, trigger the job manually via the run UI — manual runs (`/api/run/start`) bypass the linear scheduling guard entirely. The guard only applies to APScheduler-fired entry points.

---

## Table HTML cache (disk cache and TTL strategy)

Table HTML is cached to disk under `wiki_cache/` as gzipped JSON files (one per `(url, table_no)` pair). This avoids re-fetching unchanged Wikipedia pages on every run.

### TTL and batch re-check scheduling

The cache has no hard expiry by default — cached files are used as-is until explicitly invalidated. During delta runs the `cache_batch` column (assigned on office insert as `id % 7`, range 0–6) spreads conditional re-checks across 7 weekday batches:

- Each day's `today_batch` is `date.today().weekday()` (0 = Monday … 6 = Sunday).
- Offices whose `cache_batch == today_batch` are fetched with `max_age_seconds=86400` (1 day). If the cached file is more than 1 day old, the scraper sends a **conditional GET** (`If-None-Match` / `If-Modified-Since`). If Wikipedia returns HTTP 304 (not modified), the cached HTML is reused and its mtime is reset. If Wikipedia returns 200, the cache is overwritten with fresh HTML.
- All other offices use `max_age_seconds=None` — the cache is used unconditionally.

**Net effect:** each office's Wikipedia page is conditionally re-checked roughly once per week (1/7 of offices per day), while unchanged pages are served from disk without any network round-trip.

### Force refresh

Pass `refresh_table_cache=True` (via the run UI "Force refresh table cache" checkbox) to bypass the cache entirely for all offices and unconditionally re-fetch every page. Use this after a known Wikipedia restructure or when you suspect the disk cache is serving stale HTML. Under normal operations, let the `cache_batch` rotation handle freshness.

### Disk cache cleanup

The `daily_delta` job (06:00 UTC) calls `_cleanup_disk_cache(max_age_days=30)` before starting the subprocess. Any `wiki_cache/*.json.gz` file not modified in the last 30 days is deleted. The count of deleted files is included in the nightly summary email under "Cache files deleted".

### Delta vs. full run impact

- **Delta run**: uses the `cache_batch` rotation. Cache hits skip the HTTP fetch; `last_html_hash` comparisons then determine whether parsing is needed. An office whose HTML is unchanged (same hash as stored in the DB) is skipped with `offices_unchanged` incremented.
- **Full run**: `max_age_seconds=None` — disk cache is always used if present. Set `refresh_table_cache=True` to bypass it.
- **`TABLE_HTML_CACHE_ENABLED=0`**: disables the in-memory `RunPageCache` (the per-run dedup layer that avoids fetching the same URL twice within one run). The `wiki_cache/` disk cache is unaffected by this flag.
