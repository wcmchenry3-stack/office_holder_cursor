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

Stale jobs are expired by the `daily_maintenance` job (05:30 UTC) via `expire_stale_jobs()`:
- Queued jobs older than `expiry_hours_queued` (default 12h) are expired
- Running full-scrape jobs older than `expiry_hours_running_full` (default 24h) are expired
- Other running jobs older than `expiry_hours_running_other` (default 8h) are expired

Expiring a job also calls `_cancel_in_memory_job()` to stop the background thread, and sends an email notification per expired job.

For immediate recovery (e.g. a stuck job that won't expire until tomorrow's maintenance run), use `POST /api/run/force-expire-stale` — accessible via the button on `/data/scraper-jobs`.
