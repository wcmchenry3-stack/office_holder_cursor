"""
Localization utilities for RulersAI (FastAPI / Jinja2).

- SUPPORTED_LOCALES  ordered list of locale codes
- RTL_LOCALES        set of right-to-left locale codes
- LOCALE_NAMES       native-language display names
- get_translations() load and cache a Babel Translations object
- resolve_locale()   quality-weighted Accept-Language matching
"""

from functools import lru_cache
from pathlib import Path

from babel.support import Translations

# Ordered list — drives the language switcher display order.
SUPPORTED_LOCALES: list[str] = [
    "en",
    "es",
    "fr-CA",
    "de",
    "nl",
    "pt",
    "ru",
    "hi",
    "zh",
    "ja",
    "ko",
    "ar",
    "he",
]

# RTL locales are gated on story #461 (RTL layout).
# Translation files exist for all locales; the switcher hides ar/he until #461 ships.
RTL_LOCALES: frozenset[str] = frozenset({"ar", "he"})

# Display names in each locale's own script.
LOCALE_NAMES: dict[str, str] = {
    "en": "English",
    "es": "Español",
    "fr-CA": "Français (Canada)",
    "de": "Deutsch",
    "nl": "Nederlands",
    "pt": "Português",
    "ru": "Русский",
    "hi": "हिन्दी",
    "zh": "中文",
    "ja": "日本語",
    "ko": "한국어",
    "ar": "العربية",
    "he": "עברית",
}

_LOCALES_DIR = Path(__file__).resolve().parent / "locales"


@lru_cache(maxsize=64)
def get_translations(locale: str) -> Translations:
    """Load and cache a Babel Translations object for *locale*.

    Normalizes locale codes to use underscores (e.g. ``fr-CA`` → ``fr_CA``)
    so directory names match Babel's convention while HTTP locale tags use
    hyphens.  Falls back to NullTranslations if the compiled .mo file does
    not exist yet — safe for development and CI runs before ``pybabel compile``
    has been executed.
    """
    babel_locale = locale.replace("-", "_")
    try:
        t = Translations.load(str(_LOCALES_DIR), [babel_locale])
        return t
    except Exception:
        return Translations()


def resolve_locale(accept_language: str, supported: list[str] | None = None) -> str:
    """Return the best-matching supported locale from an Accept-Language header.

    Matching order:
      1. Exact match against *supported* list
      2. Base-language match  ('fr' matches 'fr-CA')
      3. 'en' fallback
    """
    if supported is None:
        supported = SUPPORTED_LOCALES
    for tag in _parse_accept_language(accept_language):
        if tag in supported:
            return tag
        base = tag.split("-")[0]
        match = next((loc for loc in supported if loc.split("-")[0] == base), None)
        if match:
            return match
    return "en"


def _parse_accept_language(header: str) -> list[str]:
    """Parse an Accept-Language header into a quality-weighted ordered list of tags.

    Example: ``'fr-CA,fr;q=0.9,en;q=0.8'`` → ``['fr-CA', 'fr', 'en']``
    """
    if not header:
        return []
    pairs: list[tuple[float, str]] = []
    for part in header.split(","):
        part = part.strip()
        if not part:
            continue
        if ";q=" in part:
            tag, q_str = part.rsplit(";q=", 1)
            try:
                q = float(q_str.strip())
            except ValueError:
                q = 1.0
        else:
            tag, q = part, 1.0
        pairs.append((-q, tag.strip()))  # negate for ascending sort
    pairs.sort(key=lambda x: x[0])
    return [tag for _, tag in pairs]
