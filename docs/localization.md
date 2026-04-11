# RulersAI — Localization (i18n)

This document covers the full localization architecture for RulersAI across web (FastAPI / Jinja2) and mobile (React Native / Expo — future). It covers locale registry, translation file structure, extraction workflow, RTL layout, testing, and the translation delivery process.

For component-level branding and visual considerations see `docs/branding.md`.  
For `lang` and `dir` attribute handling see the Infrastructure section of `docs/accessibility.md`.

---

## Supported Locales

| Code | Language | Script | Direction | Status |
|---|---|---|---|---|
| `en` | English | Latin | LTR | Source language — always complete |
| `es` | Spanish | Latin | LTR | Translate |
| `fr-CA` | French (Canadian) | Latin | LTR | Translate |
| `de` | German | Latin | LTR | Translate |
| `nl` | Dutch | Latin | LTR | Translate |
| `pt` | Portuguese | Latin | LTR | Translate |
| `ru` | Russian | Cyrillic | LTR | Translate |
| `hi` | Hindi | Devanagari | LTR | Translate |
| `zh` | Chinese (Simplified) | Han | LTR | Translate |
| `ja` | Japanese | Mixed | LTR | Translate |
| `ko` | Korean | Hangul | LTR | Translate |
| `ar` | Arabic | Arabic | **RTL** | Translate + RTL layout (gated on STORY-O) |
| `he` | Hebrew | Hebrew | **RTL** | Translate + RTL layout (gated on STORY-O) |

RTL locales (`ar`, `he`) have translation files created alongside all other locales but are not enabled in the language switcher until the RTL layout story (STORY-O) ships.

This locale set matches `book_app` and `gaming_app` exactly for consistency across the product portfolio.

---

## Architecture

### Web (FastAPI + Jinja2)

**Library:** Python `babel` + Jinja2 `i18n` extension (gettext standard)

**Why gettext, not a custom JSON loader?**  
- Industry standard for Python server-side i18n
- `pybabel` toolchain handles extraction from Jinja2 templates automatically
- `.po`/`.mo` format is translator-friendly (professional CAT tool support)
- Lazy evaluation prevents import-time issues in FastAPI

**Directory structure:**
```
src/
  locales/
    _meta/                        ← per-namespace meta files (see below)
      common.meta.json
      nav.meta.json
      offices.meta.json
      run.meta.json
      research.meta.json
      refs.meta.json
      auth.meta.json
      system.meta.json
    messages.pot                  ← master template (generated, not committed)
    en/
      LC_MESSAGES/
        messages.po               ← English source strings (committed)
        messages.mo               ← compiled binary (generated at deploy, not committed)
    es/
      LC_MESSAGES/
        messages.po
        messages.mo
    ar/
      LC_MESSAGES/
        messages.po
        messages.mo
    ... (one directory per locale)
  babel.cfg                       ← extraction configuration
```

**`babel.cfg`:**
```ini
[python: src/**.py]
[jinja2: src/templates/**.html]
extensions = jinja2.ext.i18n
encoding = utf-8
```

**Jinja2 setup in `src/main.py`:**
```python
from babel.support import Translations
from jinja2 import Environment

def get_translations(locale: str) -> Translations:
    return Translations.load('src/locales', [locale])

# In request middleware:
# 1. Detect locale (cookie → Accept-Language → 'en')
# 2. Load translations
# 3. Install into Jinja2 env via env.install_gettext_translations(t)
```

**In templates:**
```html
<!-- Simple string -->
<h1>{{ _("Office Configuration") }}</h1>

<!-- With variable interpolation -->
<p>{{ _("%(count)s offices configured", count=office_count) }}</p>

<!-- Block form (multi-word) -->
{% trans %}Save office{% endtrans %}

<!-- Plural form -->
{% trans count=num_offices %}
  {{ count }} office
{% pluralize %}
  {{ count }} offices
{% endtrans %}
```

**In Python router code:**
```python
from babel.support import LazyProxy

def lazy_gettext(string): ...  # standard lazy_gettext pattern

# Example usage:
error_msg = lazy_gettext("Office name is required.")
```

### Mobile (React Native / Expo — future)

Matches `gaming_app` and `book_app` exactly:

- **Library:** `i18next` + `react-i18next` + `i18next-resources-to-backend`
- **Device locale detection:** `expo-localization` → `resolveLocale()` with fallback chain
- **Loading:** `en` bundled statically; all other locales lazy-loaded via `import()`
- **RTL:** `I18nManager.forceRTL()` for `ar` and `he`, plus `useHtmlAttributes()` hook for web targets
- **Persistence:** `AsyncStorage` under key `rulersai_locale`

**Namespace → JSON file mapping (mirrors web namespaces):**
```
frontend/src/i18n/locales/
  en/
    common.json
    nav.json
    offices.json
    run.json
    research.json
    refs.json
    auth.json
    system.json
  es/
    common.json
    ...
```

**Key naming convention:** `namespace.component.element.type`  
Example: `offices.form.url.label`, `offices.form.url.placeholder`, `run.mode.full.label`

**Shared key names:** Web `.po` message IDs and mobile JSON keys use the same naming convention. The English source is the single source of truth; mobile JSON files are generated from `.po` files by a script at `scripts/po_to_json.py`.

---

## Namespaces

| Namespace | Coverage |
|---|---|
| `common` | Buttons (Save, Cancel, Delete, Confirm, Close, Remove), generic error titles, date/number formats, pagination labels, loading states |
| `nav` | Sidebar nav items, top bar labels, breadcrumb labels, page section labels |
| `offices` | Office list filters, office form field labels, placeholders, validation messages, table config field labels, action button labels |
| `run` | Run mode names and descriptions, option labels, progress phase labels, job status messages |
| `research` | Gemini research labels, wiki draft statuses, draft detail labels, submission options |
| `refs` | Reference data page titles and form labels (countries, states, cities, levels, branches, categories, parties, infobox filters) |
| `auth` | Login page: sign-in button, tagline, verification labels |
| `system` | Scheduled jobs labels, settings keys, runner registry labels, system admin section titles |

---

## Meta Files

Each namespace has a meta file at `src/locales/_meta/{namespace}.meta.json`.

**Purpose:** Provide context to translators and enforce constraints in CI.

**Format per key:**
```json
{
  "offices.form.url.label": {
    "description": "Label for the Wikipedia source URL input on the office edit form",
    "tone": "neutral",
    "characterLimit": 25,
    "placeholders": [],
    "doNotTranslate": ["Wikipedia"],
    "notes": null
  },
  "offices.form.name.label": {
    "description": "Label for the office name input. This is the primary identifier.",
    "tone": "neutral",
    "characterLimit": 20,
    "placeholders": [],
    "doNotTranslate": [],
    "notes": "Keep short — renders inside a compact form label above the input."
  },
  "run.progress.offices": {
    "description": "Progress label for the offices phase of a scraper run",
    "tone": "neutral",
    "characterLimit": 30,
    "placeholders": ["current", "total"],
    "doNotTranslate": [],
    "notes": "Example: 'Offices: 42 / 120'. Keep numeric format flexible."
  }
}
```

**Fields:**
- `description` — what the string is and which component/page it appears on
- `tone` — `neutral` / `action` / `functional` / `instructional` / `celebratory` / `sympathetic`
- `characterLimit` — maximum character count the UI can display without overflow
- `placeholders` — list of interpolation variables that must appear in translated string
- `doNotTranslate` — proper nouns, product names, technical terms to keep in source language
- `notes` — optional translator context

---

## Locale Detection (Web)

Detection order, evaluated on each request:

1. **Session cookie** `lang` — set by the language switcher, persists across requests
2. **`Accept-Language` HTTP header** — browser preference, matched against supported locales
3. **Fallback** — `en`

**Matching logic for `Accept-Language`:**
```python
def resolve_locale(accept_language: str, supported: list[str]) -> str:
    # Parse quality-weighted list: "fr-CA,fr;q=0.9,en;q=0.8"
    for tag in parse_accept_language(accept_language):
        if tag in supported:
            return tag
        # Base language fallback: "fr" matches "fr-CA"
        base = tag.split('-')[0]
        match = next((l for l in supported if l.split('-')[0] == base), None)
        if match:
            return match
    return 'en'
```

**Language switcher** (in top bar):
- Icon button opens a dropdown listing all active locales (name in native script, e.g. "Español", "Deutsch", "العربية")
- On selection: POST to `/set-locale` which sets the `lang` cookie and redirects back to current page
- `ar` and `he` appear in the list only after STORY-O (RTL layout) ships

---

## Extraction Workflow

```bash
# 1. Extract all translatable strings from templates and Python files
pybabel extract -F babel.cfg \
  --project=RulersAI \
  --version=1.0 \
  -o src/locales/messages.pot \
  src/

# 2a. Initialize a new locale (first time only)
pybabel init -i src/locales/messages.pot \
  -d src/locales \
  -l es

# 2b. Update existing locale with new/changed strings
pybabel update -i src/locales/messages.pot \
  -d src/locales

# 3. Translate: edit src/locales/{lang}/LC_MESSAGES/messages.po
# (or send to translator / use translation platform)

# 4. Compile to binary (done at deploy time, not committed)
pybabel compile -d src/locales

# Helper script (runs extract + update for all locales):
python scripts/i18n_update.py
```

**`scripts/i18n_update.py`** automates steps 1 + 2b and reports:
- New strings added since last extraction
- Strings removed (marked `#, obsolete` in `.po` files)
- Strings with `fuzzy` flag (changed source, needs re-translation)

**`scripts/po_to_json.py`** generates mobile JSON files from `.po` files (for React Native consumption).

---

## RTL Layout (STORY-O)

RTL layout for `ar` and `he` requires changes at multiple layers:

### HTML

```html
<!-- base.html — set by locale detection middleware -->
<html lang="{{ locale }}" dir="{{ 'rtl' if locale in RTL_LOCALES else 'ltr' }}">
```

`RTL_LOCALES = {'ar', 'he'}` — defined as a Python constant in `src/i18n.py`.

### CSS — Logical Properties

Replace all directional CSS with logical equivalents throughout `theme.css` and all component styles:

| Replace | With |
|---|---|
| `margin-left` | `margin-inline-start` |
| `margin-right` | `margin-inline-end` |
| `padding-left` | `padding-inline-start` |
| `padding-right` | `padding-inline-end` |
| `border-left` | `border-inline-start` |
| `border-right` | `border-inline-end` |
| `left: 0` | `inset-inline-start: 0` |
| `right: 0` | `inset-inline-end: 0` |
| `text-align: left` | `text-align: start` |
| `text-align: right` | `text-align: end` |
| `float: left` | `float: inline-start` |

### Sidebar

```css
/* Sidebar — flips automatically with logical properties */
nav[aria-label="Primary navigation"] {
  position: fixed;
  inset-block-start: 0;
  inset-block-end: 0;
  inset-inline-start: 0;  /* left in LTR, right in RTL */
  width: 16rem;
}

main {
  margin-inline-start: 16rem;  /* right-offset in RTL */
}
```

### Floating Outline Sidebar (Multi-Office Editor)

```css
.page-outline {
  position: fixed;
  inset-block-start: 6rem;
  inset-inline-end: 1rem;  /* right in LTR, left in RTL */
}

.page-edit-main {
  margin-inline-end: 13rem;
}
```

### Directional Icons

Icons that imply direction (chevrons, arrows, back/forward) must mirror in RTL:

```css
[dir="rtl"] .icon-directional {
  transform: scaleX(-1);
}
```

Apply `.icon-directional` class to: `chevron_right`, `chevron_left`, `arrow_forward`, `arrow_back`, `navigate_next`, `navigate_before`.

Icons that are not directional (check marks, warnings, settings, etc.) do not get this class.

### Font Stacks

```css
/* Arabic */
:lang(ar) {
  font-family: 'Inter', 'Noto Sans Arabic', 'Arial', sans-serif;
}

/* Hebrew */
:lang(he) {
  font-family: 'Inter', 'Noto Sans Hebrew', 'Arial', sans-serif;
}
```

Load Noto Sans Arabic and Noto Sans Hebrew from Google Fonts only when the active locale requires them (lazy-load via JS to avoid loading for all users).

### Number Formatting

Arabic uses Eastern Arabic numerals in some contexts. Use `Intl.NumberFormat(locale)` in JavaScript and Python's `babel.numbers.format_number(n, locale=locale)` for displayed counts and statistics — not hardcoded digit strings.

---

## Testing

### CI — Locale Smoke Tests

Render each of the 6 key pages with each of the 13 locales. Assert:
- HTTP 200 response
- No raw translation keys visible (no strings matching `^[a-z]+\.[a-z_]+\.[a-z_]+` in page body)
- Page `lang` attribute matches requested locale
- Page `dir` attribute is `rtl` for `ar` and `he`, `ltr` for all others

Pages tested: `/offices`, `/offices/new`, `/run`, `/data/wiki-drafts`, `/gemini-research`, `/login`

### CI — Character Limit Checks

`scripts/check_char_limits.py` — reads all `.po` files and compares translated string lengths against `characterLimit` in meta files. Fails CI if any translation exceeds its limit by more than 20% (buffer for natural language expansion).

### CI — Placeholder Completeness

`scripts/check_placeholders.py` — verifies that every placeholder listed in a meta `placeholders` array is present in each locale's translation. Fails CI if a placeholder is missing from a translation (would cause a runtime error).

### Manual RTL Visual Check (STORY-O)

Screenshot test for `ar` and `he` on 4 pages: offices list, edit office (single), run page, login.  
Verify:
- Sidebar appears on the right side
- Text is right-aligned
- Chevrons point the correct direction
- No text/UI overflow

### Translation Review

Before STORY-P is marked done:
- All `.po` files reviewed for completeness (no `msgstr ""` entries except intentionally empty strings)
- No `fuzzy` flags in any production locale
- `doNotTranslate` terms verified to be untranslated in each locale's `.po` file

---

## Deployment Notes

`.mo` (compiled binary) files are **not committed to git**. They are compiled at deploy time:

```bash
# In Render build command or CI deploy step:
pybabel compile -d src/locales
```

Add to `render.yaml` build command:
```yaml
buildCommand: pip install -r requirements.txt && pybabel compile -d src/locales
```

`messages.pot` (the template) is also not committed — it is regenerated on each extraction run.

Only `.po` source files and `_meta/*.meta.json` files are committed.
