# AI Pipeline Reference

Three AI services work together to maintain data quality: `ConsensusVoter` aggregates multi-provider votes, `PageQualityInspector` runs scheduled page checks, and `SuspectRecordFlagger` gates new individual insertions. Results from all three are surfaced in the `/data/ai-decisions` dashboard.

---

## ConsensusVoter (`src/services/consensus_voter.py`)

`ConsensusVoter` calls up to three AI providers in parallel and aggregates their responses into a single `ConsensusVerdict`.

### Types

```python
class Verdict(str, Enum):
    VALID               # All available providers agree the data is accurate
    INVALID             # All available providers flag an issue
    DISAGREEMENT        # Providers are split
    INSUFFICIENT_QUORUM # Fewer than 2 providers responded (no reliable verdict)

@dataclass
class AIVote:
    provider: str         # "openai", "gemini", or "claude"
    is_valid: bool | None # None if provider unavailable or errored
    concerns: list[str]   # Issues flagged by this provider
    confidence: str       # "high" | "medium" | "low"
    error: str | None     # Set if the provider call failed

@dataclass
class ConsensusVerdict:
    verdict: Verdict
    votes: list[AIVote]
    # Properties: .available_votes, .all_concerns
```

### Voting rules
- Requires **≥ 2 available providers** (those where `is_valid is not None`).
- `< 2` available → `INSUFFICIENT_QUORUM`
- All available agree valid → `VALID`
- All available agree invalid → `INVALID`
- Mixed → `DISAGREEMENT`

### System prompt
All three providers receive the same `_SYSTEM_PROMPT`:

> *"You are a data quality analyst for a political office holders database. Assess the provided record or page data and return JSON with these fields: `{"is_valid": bool, "concerns": [str], "confidence": "high"|"medium"|"low"}`. `is_valid` is true if the data appears correct and accurate."*

This ensures all providers are framed identically, making their verdicts directly comparable.

### Provider availability
A provider is skipped (returns `is_valid=None`) if its API key is not set:
- OpenAI: `OPENAI_API_KEY`
- Gemini: `GEMINI_OFFICE_HOLDER`
- Claude: `ANTHROPIC_API_KEY`

Providers are called in parallel with a configurable `timeout_s` (default 30 seconds).

### Usage

```python
voter = ConsensusVoter()
verdict = voter.vote(prompt="Is this record valid?", context={...})
if verdict.verdict == Verdict.INVALID:
    # create GitHub issue, block insertion, etc.
```

---

## PageQualityInspector (`src/services/page_quality_inspector.py`)

Runs once per day (default 09:00 UTC, configurable via `app_settings`) to compare our stored `office_terms` data against the live Wikipedia page for one source page.

### Flow

1. **Pick a page**: `pick_next_page()` selects the enabled source page least recently quality-checked (LRU via `last_quality_checked_at` on `source_pages`).
2. **Load our data**: `_load_our_data()` fetches all `office_terms` for that source page from our DB.
   - Returns `None` on DB error → result `fetch_failed`; page is NOT marked checked (will be retried next run)
   - Returns `[]` if our DB has zero records for this page → result `no_data` (see below)
3. **Fetch live Wikipedia HTML**: downloads the current page to compare against our stored data.
4. **Build prompt**: constructs a comparison prompt including our record count and data.
5. **Call ConsensusVoter**: sends the prompt to all configured providers.
6. **Record result**: writes to `page_quality_checks`; marks `source_pages.last_quality_checked_at`.

### Result codes

| Code | Meaning | GitHub issue created? |
|---|---|---|
| `ok` | Consensus says data is accurate | No |
| `reparse_ok` | Consensus said invalid; re-parse fixed it | No |
| `gh_issue` | Consensus says invalid; re-parse did not help | Yes |
| `manual_review` | Providers disagree (`DISAGREEMENT`) | Yes |
| `no_data` | Our DB has zero records for this page (AI skipped, no token spend) | Yes |
| `fetch_failed` | DB error loading our data; page NOT marked checked | No |
| `skipped_no_data` | All 10 retry attempts were `no_data`; skipping today | No |
| `no_pages` | No enabled source pages exist | No |

### No-data path
If our DB has zero records for a page (`[]`), sending empty data to three AIs wastes tokens. The inspector skips the AI call, creates a GitHub issue to flag the empty page, and marks the page checked. The daily job retries up to **10 different pages** (`_PAGE_QUALITY_MAX_ATTEMPTS = 10`) before recording `skipped_no_data` for the day.

### Re-parse path
When `ConsensusVoter` returns `INVALID`, the inspector triggers a full re-parse of the source page before concluding. If the re-parse produces updated data that the AI then accepts, the result is `reparse_ok`. Otherwise `gh_issue`.

### Kill switches
- `RUNNERS_ENABLED` env var (global — disables all scheduled jobs without a deploy)
- Per-job pause via `/data/scheduled-jobs` UI (stored in `scheduler_settings` table; survives restart)

---

## SuspectRecordFlagger (`src/services/suspect_record_flagger.py`)

Gates individual insertions via a pre-insertion AI check. Called from `runner.py` before inserting each new individual.

### Entry point

```python
result = check_and_gate(full_name, wiki_url, office_id, conn=conn)
# Returns: "allowed" | "skipped" | "gh_issue"
```

If `result != "allowed"`, the insertion is blocked. The scraper logs the outcome and continues with the next row.

### Result codes

| Code | Meaning |
|---|---|
| `allowed` | Passed all pattern checks; insertion proceeds |
| `skipped` | Flagged as suspect; insertion blocked; no GitHub issue |
| `gh_issue` | Flagged as suspect; insertion blocked; GitHub issue created |

All outcomes are recorded to the `suspect_record_flags` table for audit visibility.

---

## `/data/ai-decisions` dashboard

A unified read-only view aggregating records from all four AI decision tables via a UNION SQL query.

### Decision types

| Type | Source table | Subject shown |
|---|---|---|
| `data_quality` | `data_quality_reports` | Record fingerprint |
| `parse_error` | `parse_error_reports` | Wikipedia URL |
| `page_quality` | `page_quality_checks` | Source page URL (clickable) |
| `suspect_flag` | `suspect_record_flags` | Individual full name |

### Filters
- `?type=<decision_type>` — filter to one decision type; invalid values are silently ignored
- `?result=<result_code>` — filter to a specific result/action code
- `?offset=<n>` — pagination offset

### AI votes column
For `page_quality` rows, a collapsible `<details>` block shows each provider's individual vote: `accurate`, `inaccurate`, or `unavailable: <error>`.

### Result code reference

| Code | Badge color | Meaning |
|---|---|---|
| `ok` | Green | Data confirmed accurate |
| `reparse_ok` | Green | Invalid flag resolved by re-parse |
| `allowed` | Green | Suspect gate passed |
| `gh_issue` | Orange | Issue flagged; GitHub issue created |
| `skipped` | Orange | Flagged; no issue (below GH issue threshold) |
| `manual_review` | Orange | AI providers disagree; needs human check |
| `no_data` | Orange | Page has no records in our DB |
| `skipped_no_data` | Orange | All retry attempts were no_data |
| `fetch_failed` | Red | DB error; could not load our data |
| `error` | Red | Unhandled error |
