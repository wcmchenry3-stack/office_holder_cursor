# CLAUDE.md — Office Holder

Single-user FastAPI/SQLite app that scrapes Wikipedia tables to build a political office holders database. Deployed on Render.com (persistent disk). See `docs/` for full references.

## Quick Start (local dev)

```bash
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn src.main:app --reload                     # http://127.0.0.1:8000
```

Auth is bypassed locally when `GOOGLE_CLIENT_ID` is not set.

## Key Mental Model: Page → Office → Table

```
source_pages → office_details → office_table_config
```

A Wikipedia URL → a logical office on that page → how to parse one HTML table.

**Critical:** The legacy flat `offices` table is still in active use — all scraper runs read from it. The hierarchy tables were added later and coexist. When adding new config fields, add to **both** `offices` AND `office_table_config`, with a migration for each.

## Schema / Migration Rule

Never alter the schema manually. Add a migration function to `src/db/migrate.py` and call it from `migrate_to_fk()`. Migrations must be idempotent. See `docs/schema.md`.

## Git Workflow

Branches: `feature/<name>` or `bug/<name>`. Never push directly to `dev`.

**Start of every task:**
```bash
git push                                          # push current branch first
git checkout dev && git pull origin dev
git checkout -b feature/<name> && git push -u origin feature/<name>
```

Finish: run tests → push final commits → open PR `feature/<name>` → `dev` on GitHub.

**Before every push, run the full test suite and confirm it passes:**
```bash
python -m pytest
```
All non-Playwright tests must pass (Playwright tests skip automatically unless the app is running).

## Documentation

| File | Contents |
|---|---|
| `docs/architecture.md` | Architecture diagram, auth, async jobs, directory tree, dev setup, env vars |
| `docs/run-modes.md` | All 7 run modes, auto-table-update algorithm, infobox lookup conditions |
| `docs/schema.md` | Schema diagram, table reference, 23-migration history |
| `docs/config-options.md` | All `office_table_config` fields and infobox filter syntax |
| `docs/conventions.md` | Coding conventions, parsing gotchas, testing infrastructure, technical debt |
| `README.md` | User-facing setup, UI walkthrough, route reference |
