# CLAUDE.md — Office Holder
<!-- Global standards: ~/.claude/CLAUDE.md and ~/.claude/standards/ -->
<!-- Keep this file ≤ 50 lines. Move detail to docs/ rather than expanding here. -->

Single-user FastAPI/PostgreSQL app that scrapes Wikipedia tables to build a political office holders database. Deployed at `rulersai.buffingchi.com` (prd) with a future dev environment at `dev-rulersai.buffingchi.com`. See `docs/` for full references.

## Quick Start (local dev)

```bash
python -m venv venv && source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn src.main:app --reload                     # http://127.0.0.1:8000
```

Auth is bypassed locally when `GOOGLE_CLIENT_ID` is not set.

## Key Mental Model: Page → Office → Table

`source_pages → office_details → office_table_config` — a Wikipedia URL → a logical office → how to parse one HTML table. When adding config fields, add to **both** `offices` AND `office_table_config` with a migration. Never alter schema manually — use `src/db/migrate.py`. See `docs/schema.md`.

## Git Workflow

Branches: `feature/<name>` or `bug/<name>`. Never push directly to `dev`.

```bash
git push && git checkout dev && git pull origin dev
git checkout -b feature/<name> && git push -u origin feature/<name>
```

Before every push: `python -m pytest` — all non-Playwright tests must pass.

Finish: push commits → open PR `feature/<name>` → `dev` on GitHub.

## Available Agents

These project subagents live in `.claude/agents/` and are invoked via the `Agent` tool (not Skill). Always prefer them over a general-purpose agent for their domain.

| Agent | `subagent_type` | When to use |
|---|---|---|
| lint-review | `lint-review` | Auto-fix lint issues after a lint-gate hook failure |
| plan-issues | `plan-issues` | Break a feature/bug/initiative into scoped GitHub issues — investigates code first, drafts for confirmation, then calls `gh issue create` |
| policy-compliance | `policy-compliance` | Check and fix policy violations after a policy-gate hook failure |

## Documentation

| File | Contents |
|---|---|
| `docs/architecture.md` | Auth, async jobs, directory tree, env vars |
| `docs/run-modes.md` | All run modes, auto-table-update, infobox lookup |
| `docs/schema.md` | Schema diagram, migration history |
| `docs/security.md` | OWASP-aligned pen test policy and security tests |
| `docs/conventions.md` | Coding conventions, testing infrastructure |
