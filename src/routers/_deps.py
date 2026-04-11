"""Shared dependencies for all routers."""

from pathlib import Path
from typing import Any

import jinja2
from fastapi.templating import Jinja2Templates
from slowapi import Limiter
from slowapi.util import get_remote_address
from starlette.requests import Request as StarletteRequest

from src.i18n import RTL_LOCALES, get_translations

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"

# Pre-configure a Jinja2 Environment with the i18n extension so Starlette's
# Jinja2Templates doesn't receive extra env options (which were deprecated
# in Starlette ≥ 0.36).
_jinja_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
    extensions=["jinja2.ext.i18n"],
    autoescape=True,
)


class I18nTemplates(Jinja2Templates):
    """Jinja2Templates subclass that injects per-request i18n context.

    On each TemplateResponse call the following variables are added to the
    template context (unless already present):

    - ``_``         gettext shorthand for ``{{ _("string") }}``
    - ``gettext``   same function (used by ``{% trans %}`` blocks)
    - ``ngettext``  plural-form gettext
    - ``locale``    active locale code, e.g. ``'en'``, ``'fr-CA'``
    - ``dir``       ``'rtl'`` for Arabic/Hebrew, ``'ltr'`` for all others
    """

    def TemplateResponse(
        self,
        request: StarletteRequest,
        name: str,
        context: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        ctx = context or {}
        locale: str = getattr(request.state, "locale", "en")
        t = get_translations(locale)
        ctx.setdefault("_", t.gettext)
        ctx.setdefault("gettext", t.gettext)
        ctx.setdefault("ngettext", t.ngettext)
        ctx.setdefault("locale", locale)
        ctx.setdefault("dir", "rtl" if locale in RTL_LOCALES else "ltr")
        return super().TemplateResponse(request, name, ctx, **kwargs)


templates = I18nTemplates(env=_jinja_env)


def _rate_limit_key(request: StarletteRequest) -> str:
    """Key by authenticated user email, or client IP for unauthenticated routes.

    Authenticated routes key by user_email per project rule (single-user app).
    Unauthenticated routes (auth callbacks) key by IP for DoS protection.
    """
    email = getattr(request, "session", {}).get("user_email", "")
    return email if email else get_remote_address(request)


limiter = Limiter(key_func=_rate_limit_key, default_limits=["200/minute"])
