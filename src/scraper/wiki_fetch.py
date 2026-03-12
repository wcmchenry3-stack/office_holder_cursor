# -*- coding: utf-8 -*-
"""
Wikipedia fetch helpers: REST API URL and shared request headers.
Uses Wikimedia REST API for HTML when possible (policy-friendly and CDN-cached).
On 429 Too Many Requests, callers should respect the Retry-After header before retrying.
"""

from urllib.parse import urlparse, unquote, urlunparse

from src.scraper.logger import HTTP_USER_AGENT

# Headers for all Wikipedia requests (User-Agent + gzip per Wikimedia policy).
WIKIPEDIA_REQUEST_HEADERS = {
    "User-Agent": HTTP_USER_AGENT,
    "Accept-Encoding": "gzip",
}


def normalize_wiki_url(wiki_url: str) -> str | None:
    """
    Normalize a Wikipedia URL: strip trailing dot from host (e.g. en.wikipedia.org. -> en.wikipedia.org)
    and ensure path has /wiki/ prefix (e.g. /Thomas_Van_Lear -> /wiki/Thomas_Van_Lear).
    Returns normalized URL or None if not a Wikipedia URL.
    """
    if not (wiki_url or "").strip():
        return None
    try:
        p = urlparse(wiki_url.strip())
    except Exception:
        return None
    netloc = (p.netloc or "").rstrip(".")
    if "wikipedia.org" not in netloc:
        return None
    path = (p.path or "").strip().rstrip("/")
    parts = [x for x in path.split("/") if x]
    if len(parts) == 1:
        path = f"/wiki/{parts[0]}"
    return urlunparse((p.scheme or "https", netloc, path, p.params, p.query, p.fragment))


def canonical_holder_url(url: str) -> str:
    """
    Canonicalize holder URL for comparisons (e.g. matching existing terms to table rows).
    Normalizes via normalize_wiki_url and produces a stable /wiki/<title> key (lowercased)
    so scheme/host/query/encoding/case differences don't create false mismatches.
    """
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("No link:"):
        return u
    normalized = normalize_wiki_url(u)
    if normalized:
        try:
            p = urlparse(normalized)
            path = (p.path or "").rstrip("/")
            parts = [x for x in path.split("/") if x]
            if len(parts) >= 2 and parts[0].lower() == "wiki":
                title = unquote(parts[1]).replace(" ", "_").strip().lower()
                return f"/wiki/{title}"
            return urlunparse(("https", (p.netloc or "").lower(), path, "", "", ""))
        except Exception:
            return normalized
    return u


def wiki_url_to_rest_html_url(wiki_url: str) -> str | None:
    """
    If wiki_url is a Wikipedia page URL, return the REST API HTML URL for the same page.
    Otherwise return None.
    Example: https://en.wikipedia.org/wiki/Barack_Obama -> https://en.wikipedia.org/w/rest.php/v1/page/Barack_Obama/html
    """
    normalized = normalize_wiki_url(wiki_url)
    if not normalized:
        return None
    try:
        p = urlparse(normalized)
    except Exception:
        return None
    path = (p.path or "").strip().rstrip("/")
    parts = [x for x in path.split("/") if x]
    if len(parts) >= 2 and parts[0].lower() == "wiki":
        title = unquote(parts[1])
        if not title:
            return None
        scheme = p.scheme or "https"
        netloc = (p.netloc or "en.wikipedia.org").rstrip(".")
        return f"{scheme}://{netloc}/w/rest.php/v1/page/{title}/html"
    return None
