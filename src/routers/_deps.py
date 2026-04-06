"""Shared dependencies for all routers."""

from pathlib import Path

from fastapi.templating import Jinja2Templates
from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.requests import Request as StarletteRequest

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _rate_limit_key(request: StarletteRequest) -> str:
    """Key by authenticated user email, or client IP for unauthenticated routes.

    Authenticated routes key by user_email per project rule (single-user app).
    Unauthenticated routes (auth callbacks) key by IP for DoS protection.
    """
    email = getattr(request, "session", {}).get("user_email", "")
    return email if email else get_remote_address(request)


limiter = Limiter(key_func=_rate_limit_key, default_limits=["200/minute"])
