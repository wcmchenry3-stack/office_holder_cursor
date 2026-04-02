# Security Policy — Office Holder

See [~/.claude/standards/security.md](~/.claude/standards/security.md) for the universal OWASP Top 10 framework and secret hygiene rules.

## Project-Specific Implementation

All code changes must be tested against this policy before merging. The automated suite lives in
`src/test_security.py` and runs as part of the normal pytest suite:

```bash
python -m pytest src/test_security.py -v   # security tests only
pytest -m security -v                       # same, by marker
python -m pytest                            # included in full suite
```

---

## OWASP Top 10 Coverage

### A01 — Broken Access Control
- **Policy**: Every non-public route must redirect unauthenticated users to `/login` when
  `GOOGLE_CLIENT_ID` is set. Public paths are limited to `/login`, `/auth/google`,
  `/auth/google/callback`, and `/static/`.
- **Test**: `test_auth_middleware_redirects_unauthenticated_requests`,
  `test_auth_middleware_allows_public_paths`
- **Dev note**: Auth is intentionally disabled locally when `GOOGLE_CLIENT_ID` is unset. Never
  deploy without it.

### A02 — Cryptographic Failures
- **Policy**: `SECRET_KEY` env var must be set to a strong random value in production. The default
  `"dev-only-insecure-key"` value must never be used in a deployed environment.
- **Test**: Manual (env var audit before deploy). No automated test — asserting the key is secret
  would require embedding it in the test.

### A03 — Injection
- **Policy**: All database queries must use parameterised statements (`?` placeholders). No
  `f`-string SQL, no `str.format()` in queries. Filter criteria are matched as plain substrings —
  never passed to `eval`, `exec`, or a regex engine controlled by user input.
- **Test**: `test_sql_injection_in_offices_query_params` — submits classic payloads (`'; DROP TABLE
  offices; --`, `UNION SELECT`, `OR '1'='1'`) and asserts the app returns 200 with the DB intact.

### A04 — Insecure Design
- **Policy**: All subprocess calls must use list-based arguments (no `shell=True`). Wikipedia fetch
  URLs must be validated through `normalize_wiki_url()` before any HTTP call. The Datasette
  instance is bound to `127.0.0.1` only and mounted read-only (`--immutable`).
- **Test**: Covered by existing integration tests; `test_run_api_invalid_run_mode_does_not_crash`
  fuzzes the `run_mode` field with an XSS payload.

### A05 — Security Misconfiguration
- **Policy**: Every HTTP response must carry:
  - `X-Frame-Options: DENY`
  - `X-Content-Type-Options: nosniff`
  - `Referrer-Policy: strict-origin-when-cross-origin`
  These are added by the `add_security_headers` middleware in `src/main.py`.
- **Test**: `test_security_headers_present`

### A06 — Vulnerable and Outdated Components
- **Policy**: Dependencies are pinned in `requirements.txt`. Run `pip list --outdated` before
  each release cycle and update dependencies with known CVEs.
- **Test**: Manual / CI dependency scanning (not yet automated).

### A07 — Identification and Authentication Failures
- **Policy**: Authentication uses Google OAuth via Authlib. Sessions are signed with
  `SessionMiddleware` using a secret set via `SECRET_KEY`. Only the single email address in
  `ALLOWED_EMAIL` may log in.
- **Test**: `test_auth_middleware_redirects_unauthenticated_requests`

### A08 — Software and Data Integrity Failures
- **Policy**: CSV imports validate file extension (`.csv` only). All uploads are written to
  `tempfile.NamedTemporaryFile` and deleted after use. No deserialisation of untrusted data
  (no `pickle`, no `yaml.load` without `Loader=SafeLoader`).
- **Test**: `test_csv_upload_rejects_non_csv_extension`

### A09 — Security Logging and Monitoring Failures
- **Policy**: Scraper runs log to timestamped files under `data/logs/`. Auth failures are logged
  by FastAPI's default error handler. No PII is written to log files.
- **Test**: Out of scope for automated tests; review log output manually.

### A10 — Server-Side Request Forgery (SSRF)
- **Policy**: All outbound HTTP calls target Wikipedia's REST API, OpenAI API, or Google Gemini API only.
  `normalize_wiki_url()` enforces `https` scheme and `wikipedia.org` in the hostname before any
  request is made. User-supplied URLs in `individual_ref` are passed through this validator.
  Gemini API calls go through `src/services/gemini_vitals_researcher.py` only; OpenAI through
  `src/services/ai_office_builder.py` only.
- **Test**: `test_run_api_requires_individual_ref_for_single_bio_mode` (boundary check);
  full URL validation tested in `src/scraper/test_wiki_fetch.py` if present.
- **Gemini API note**: Google retains prompts and responses for 55 days for abuse monitoring.
  Do not submit sensitive or personal data via the Gemini API on the free tier.

---

## Additional Controls

### Path Traversal
- **Policy**: Error messages must never include resolved absolute filesystem paths. Relative paths
  submitted via form fields are resolved to the project root — the resolved path is not returned
  to the user.
- **Test**: `test_offices_import_error_does_not_leak_resolved_path`,
  `test_offices_import_rejects_empty_path`

### Type Safety
- **Policy**: FastAPI path parameters with integer types are validated automatically (HTTP 422 on
  type mismatch). No manual casting of path params from strings in route handlers.
- **Test**: `test_integer_path_param_type_safety`

---

## Pre-commit Checklist

Before opening a PR:

- [ ] `python -m pytest` — full suite passes
- [ ] `pytest -m security -v` — all 10 security tests pass
- [ ] No new `shell=True` subprocess calls introduced
- [ ] No raw f-string SQL queries introduced
- [ ] No new public routes added without updating `_PUBLIC_PATHS` intentionally
- [ ] `SECRET_KEY` and `GOOGLE_CLIENT_ID` confirmed set in deployment environment
