"""Shared dependencies for all routers."""

import subprocess
from pathlib import Path

from fastapi.templating import Jinja2Templates
from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.requests import Request as StarletteRequest

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
ROOT = Path(__file__).resolve().parent.parent.parent

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _rate_limit_key(request: StarletteRequest) -> str:
    """Key by authenticated user email, or client IP for unauthenticated routes.

    Authenticated routes key by user_email per project rule (single-user app).
    Unauthenticated routes (auth callbacks) key by IP for DoS protection.
    """
    email = getattr(request, "session", {}).get("user_email", "")
    return email if email else get_remote_address(request)


limiter = Limiter(key_func=_rate_limit_key, default_limits=["200/minute"])


def _run_git_command(args: list[str]) -> str:
    """Run a git command at repo root and return stripped stdout (empty on error)."""
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
            text=True,
        )
        return (result.stdout or "").strip()
    except Exception:
        return ""


def _get_git_sync_status() -> dict:
    """Return git sync metadata for UI banner display."""
    inside_repo = _run_git_command(["rev-parse", "--is-inside-work-tree"]) == "true"
    if not inside_repo:
        return {"unsynced": False}

    branch = _run_git_command(["symbolic-ref", "--quiet", "--short", "HEAD"]) or "(detached HEAD)"
    upstream = _run_git_command(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    dirty = bool(_run_git_command(["status", "--porcelain"]))
    ahead = 0
    behind = 0

    if upstream:
        counts = _run_git_command(["rev-list", "--left-right", "--count", f"HEAD...{upstream}"])
        parts = counts.split()
        if len(parts) == 2:
            try:
                behind = int(parts[0])
                ahead = int(parts[1])
            except ValueError:
                ahead = 0
                behind = 0

    unsynced = dirty or ahead > 0 or not upstream
    if not unsynced:
        return {"unsynced": False}

    if not upstream:
        message = (
            "Local changes are not synced to a remote branch yet. "
            "Create/push a feature branch (for example: git push -u origin "
            f"{branch}) because direct pushes to dev are blocked."
        )
    elif dirty:
        message = (
            "You have local edits not yet committed. Commit to this feature branch, "
            "then push to sync with remote."
        )
    else:
        message = (
            f"You are {ahead} commit(s) ahead of {upstream}. "
            "Push this feature branch to sync remote."
        )

    return {
        "unsynced": True,
        "branch": branch,
        "upstream": upstream,
        "ahead": ahead,
        "behind": behind,
        "dirty": dirty,
        "message": message,
    }


templates.env.globals["git_sync_status"] = _get_git_sync_status
