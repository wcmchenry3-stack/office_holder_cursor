# RulersAI — Branding Guidelines

> Single source of truth for color, typography, iconography, and component patterns.
> Covers web and mobile. Apply to all new templates and any redesigned pages.

---

## 1. Identity

**Name:** RulersAI  
**Tagline:** Institutional Intelligence  
**Tone:** Authoritative, precise, data-forward. Not playful, not corporate-generic.  
**Logo mark:** Chihuahua wearing a gold crown inside a dark circular badge with circuit-board detail.  
**Wordmark:** `RulersAI` — `Rulers` in deep navy, `AI` in metallic gold.  
**Asset:** `src/static/images/rulersai-icon.png`

### Logo Usage Rules
- Never recolor the logo mark.
- Minimum clear space: equal to the height of the crown on all sides.
- On dark backgrounds: use the icon as-is (it has a dark badge that works on dark surfaces).
- Do not stretch, rotate, or add drop shadows to the logo.
- In the sidebar, display the text wordmark `RulersAI` in `font-black` + `tracking-tighter` alongside a small icon.

---

## 2. Color System (Material You)

The palette is a Material Design 3 tonal system with two roles: **light** and **dark**.
All tokens map directly to Tailwind CSS custom colors.

### Light Mode

| Token | Hex | Usage |
|---|---|---|
| `primary` | `#041627` | Headings, active nav, high-emphasis text |
| `primary-container` | `#1a2b3c` | Dark navy masthead backgrounds, primary CTA |
| `on-primary` | `#ffffff` | Text on primary-container |
| `on-primary-container` | `#8192a7` | Muted text on dark surfaces, secondary stats |
| `primary-fixed` | `#d2e4fb` | Icon container backgrounds, light accent fills |
| `primary-fixed-dim` | `#b7c8de` | Disabled states, placeholder icons |
| `secondary` | `#545f72` | Secondary nav text, metadata labels |
| `secondary-container` | `#d5e0f7` | Chip backgrounds, secondary badges |
| `background` | `#f7fafc` | Page background |
| `surface` | `#f7fafc` | Card surface |
| `surface-container-lowest` | `#ffffff` | Elevated card background |
| `surface-container-low` | `#f1f4f6` | Input fields, sidebar background |
| `surface-container` | `#ebeef0` | Dividers, nested containers |
| `surface-container-high` | `#e5e9eb` | Hover states |
| `surface-container-highest` | `#e0e3e5` | Strongly pressed states |
| `surface-dim` | `#d7dadc` | Disabled surface |
| `surface-bright` | `#f7fafc` | Top app bar |
| `on-surface` | `#181c1e` | Body text |
| `on-surface-variant` | `#44474c` | Secondary body text, captions |
| `outline` | `#74777d` | Input borders, dividers |
| `outline-variant` | `#c4c6cd` | Subtle dividers |
| `tertiary` | `#00162c` | Deep accent (sparingly) |
| `tertiary-container` | `#002b4e` | Status chips on dark backgrounds |
| `on-tertiary-container` | `#4894e2` | Steel blue accent — progress, active states |
| `error` | `#ba1a1a` | Destructive actions, validation errors |
| `error-container` | `#ffdad6` | Error badge backgrounds |
| `inverse-surface` | `#2d3133` | Tooltip / snackbar backgrounds |
| `inverse-on-surface` | `#eef1f3` | Tooltip text |

### Dark Mode

| Token | Hex |
|---|---|
| `background` | `#0b1326` |
| `surface` | `#0b1326` |
| `surface-container-lowest` | `#060e20` |
| `surface-container-low` | `#131b2e` |
| `surface-container` | `#171f33` |
| `surface-container-high` | `#222a3d` |
| `surface-container-highest` | `#2d3449` |
| `primary` | `#8ed5ff` |
| `primary-container` | `#38bdf8` |
| `on-primary-container` | `#7bd0ff` |
| `secondary` | `#b9c8de` |
| `secondary-container` | `#39485a` |
| `outline` | `#87929a` |
| `outline-variant` | `#3e484f` |
| `on-surface` | `#e2e8f0` |
| `error` | `#ffb4ab` |
| `error-container` | `#93000a` |

### Semantic Color Usage

| Semantic | Light | Dark | Use case |
|---|---|---|---|
| Enabled / success | `#16a34a` (green-600) | `#4ade80` | Toggle on, enabled badge |
| Warning | `#d97706` (amber-600) | `#fbbf24` | Pending updates, stale jobs |
| Running / active | `on-tertiary-container` `#4894e2` | same | Job in progress |
| Disabled | `outline` `#74777d` | same | Toggle off, disabled office |
| Destructive | `error` `#ba1a1a` | `#ffb4ab` | Delete buttons |

### Gold Accent (Logo only)
`#C9A84C` — the crown/`AI` gold. **Only use in the logo wordmark and icon.** Do not use as a UI color.

---

## 3. Typography

**Single font family: Inter** (Google Fonts)

```html
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
```

| Role | Weight | Size | Letter-spacing | Use |
|---|---|---|---|---|
| Display / Hero | 800–900 | 2.5–3rem | `tracking-tighter` | Page masthead titles |
| Headline | 700 | 1.25–1.5rem | `tracking-tight` | Card headers, section titles |
| Title | 600 | 1rem | normal | Sub-section titles |
| Label | 700 | 0.625rem (10px) | `tracking-widest` (0.1em+) | Field labels, nav items, badges |
| Body | 500 | 0.875rem (14px) | normal | Table data, form values |
| Caption | 500 | 0.75rem (12px) | `tracking-wide` | Helper text, timestamps |

**Label style rule:** All form field labels, navigation items, and badge text use `text-[10px] font-bold uppercase tracking-widest`. This is the single most distinctive typographic pattern in the design.

---

## 4. Iconography

**Library: Material Symbols Outlined**

```html
<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:wght,FILL@100..700,0..1&display=swap" rel="stylesheet">
```

```css
.material-symbols-outlined {
    font-variation-settings: 'FILL' 0, 'wght' 400, 'GRAD' 0, 'opsz' 24;
}
/* Filled variant (active/selected states): */
.material-symbols-outlined.filled {
    font-variation-settings: 'FILL' 1, 'wght' 400, 'GRAD' 0, 'opsz' 24;
}
```

### Navigation Icon Map

| Screen | Icon |
|---|---|
| Operations / Dashboard | `dashboard` |
| Offices | `domain` |
| Run / Scraper | `terminal` |
| Data / Individuals | `groups` |
| Wiki Drafts | `description` |
| Gemini Research | `person_search` |
| AI Offices | `auto_awesome` |
| Reference Data | `database` |
| Reports | `analytics` |
| System / Settings | `settings` |
| Logout | `logout` |
| Support | `help` |

---

## 5. Layout Shell

### Web (≥768px)

```
┌─────────────────────────────────────────────────────────┐
│  TopAppBar  (h-16, sticky, z-30, bg-surface-bright)     │
├───────────┬─────────────────────────────────────────────┤
│           │                                             │
│  Sidebar  │  Main Content                               │
│  w-64     │  ml-64, pb-24                               │
│  fixed    │                                             │
│  h-screen │  [DataMasthead: bg-primary-container]       │
│           │  [Card Grid: -mt-16, z-20, space-y-8]       │
│           │                                             │
└───────────┴─────────────────────────────────────────────┘
```

**Sidebar:**
- Background: `bg-[#F1F4F6]` light / `bg-[#0b1326]` dark
- Logo area: icon + "RulersAI" wordmark + "Institutional Intelligence" caption
- Nav items: `text-[10px] font-bold uppercase tracking-wider`
- Active state: `border-l-4 border-primary font-bold text-primary` (light) / `border-r-2 border-primary` (dark)
- Hover state: `hover:bg-[#E2E8F0]` light / `hover:bg-[#131b2e]` dark
- Bottom: "New Office" CTA button (full width, `bg-primary-container text-white rounded`) + Support + Logout

**Top App Bar:**
- Background: `bg-[#F7FAFC]` / `bg-[#0b1326]` dark
- Global search input: `bg-surface-container-low rounded-full pl-10` with search icon
- Right: notifications icon, settings icon, user avatar (32px circle)
- No secondary tab-bar (the design template included "Intelligence/Campaigns/Legislative/Archives" tabs — these are **not implemented**; the app uses the sidebar for all navigation)

**Data Masthead (signature element on every main page):**
- Background: `bg-primary-container` (dark navy)
- Content: eyebrow label (10px uppercase), large title (3rem bold), optional body text
- Floating stat cards: `backdrop-blur-xl bg-white/5 border border-white/10 p-5 rounded-lg`
- The main content cards are positioned with `-mt-16 relative z-20` to overlap the masthead bottom edge

### Mobile (<768px)

- Sidebar hidden by default, opens as an overlay drawer via hamburger button
- Top app bar: hamburger (left), logo (center), avatar (right)
- Bottom navigation bar: 5 key items (Dashboard, Offices, Run, Research, More)
- Data tables collapse to card view (one card per row)
- Data masthead: full width, no floating stat cards (stats stack vertically below title)
- Touch targets: minimum 44×44px for all interactive elements
- Font size floor: 14px for body, 12px for captions — no smaller

---

## 6. Component Patterns

### Cards
```
bg-surface-container-lowest
rounded-xl
shadow-[0px_12px_32px_rgba(24,28,30,0.06)]
p-8
```

### Section Headers (inside cards)
```
w-10 h-10 rounded-full bg-primary-fixed flex items-center justify-center
+ material symbol icon text-primary
+ h3 text-xl font-bold tracking-tight text-primary
```

### Form Field Labels
```
text-[10px] font-bold uppercase tracking-widest text-on-secondary-container mb-2
```

### Inputs
```
bg-surface-container-low border-none rounded-lg
focus:ring-2 focus:ring-primary
py-3 px-4 font-medium text-sm
```

### Buttons

| Variant | Classes |
|---|---|
| Primary | `bg-primary-container text-white font-bold py-3 px-6 rounded hover:opacity-90` |
| Secondary | `bg-surface-container-low text-primary font-bold py-2 px-4 rounded hover:bg-surface-container-high` |
| Danger | `bg-error text-white font-bold py-2 px-4 rounded hover:opacity-90` |
| Ghost / Icon | `text-on-surface-variant hover:bg-surface-container-low p-2 rounded-full` |
| Small | Add `text-xs py-1.5 px-3` |

### Badges / Status Pills

| State | Classes |
|---|---|
| Enabled (green) | `bg-green-100 text-green-800 text-[10px] font-bold px-2 py-0.5 rounded-full uppercase tracking-wide` |
| Disabled (gray) | `bg-surface-container text-outline text-[10px] font-bold px-2 py-0.5 rounded-full uppercase tracking-wide` |
| Running (blue) | `bg-tertiary-container text-on-tertiary-container text-[10px] font-bold px-3 py-1 rounded-full uppercase tracking-widest` |
| Error (red) | `bg-error-container text-on-error-container text-[10px] font-bold px-2 py-0.5 rounded-full uppercase tracking-wide` |
| Pending (amber) | `bg-amber-100 text-amber-800 text-[10px] font-bold px-2 py-0.5 rounded-full uppercase tracking-wide` |

### Toggles (Enable/Disable)
Use a styled checkbox or custom toggle:
- On: `bg-primary-container` thumb slides right
- Off: `bg-surface-container-highest` thumb left
- Always include visible label text + accessible `aria-label`

### Data Tables
- Header row: `text-[10px] font-bold uppercase tracking-widest text-on-secondary-container bg-surface-container-low`
- Row: `border-b border-outline-variant/20 hover:bg-surface-container-low transition-colors`
- Actions column: icon buttons with tooltips, right-aligned

### Border Radius Scale
```
DEFAULT: 2px    (inputs, small elements)
lg:      4px    (buttons)
xl:      8px    (cards, modals)
full:    12px   (pills, chips, avatars)
```

---

## 7. Dark Mode

Toggle via `class="dark"` on `<html>`. Persist in `localStorage`.
Toggle button in top app bar (moon/sun icon).

Dark background: `#0b1326` — a very deep midnight navy, not pure black.
Dark sidebar: same `#0b1326`, blending into the content area.
Dark cards: `bg-[#131b2e]` with subtle `border border-outline-variant/20`.

---

## 8. Navigation Mapping

The design tool used aspirational nav labels. This table maps design labels to actual routes:

| Design label | Actual route | Icon |
|---|---|---|
| Dashboard | `/operations` | `dashboard` |
| Offices | `/offices` | `domain` |
| Command Center | `/run` | `terminal` |
| Intelligence Data | `/data/individuals` | `groups` |
| Wiki Drafts | `/data/wiki-drafts` | `description` |
| Deep Research | `/gemini-research` | `person_search` |
| AI Creation | `/ai-offices` | `auto_awesome` |
| Reference Data | `/refs` | `database` |
| Reports | `/reports` | `analytics` |
| System | `/data/scheduled-jobs` | `settings` |

The "New Research Report" CTA in the sidebar maps to **"New Office"** (`/offices/new`).

The top-bar secondary tab nav (Intelligence / Campaigns / Legislative / Archives) from the design templates is **not implemented** — these labels have no equivalent routes.

---

## 9. Mobile-First Considerations

The mobile designs cover: Login, Dashboard, Offices List, Scraper Control, Gemini Research, Wiki Drafts.

Key mobile-specific patterns:
- **Bottom nav bar**: 5 items (Dashboard, Offices, Run, Research, More)
- **Drawer**: Full-height overlay sidebar, dismiss on backdrop tap
- **Cards over tables**: On screens < 640px, list views render as stacked cards instead of tables
- **Masthead**: Full-bleed, title only, stat cards stack below (no float)
- **Form sections**: Accordion/collapsible — only the active section is expanded
- The heavy "Table Extraction Config" section in the office editor is **web-only** in v1 (collapse to read-only summary on mobile with a "Open on desktop" prompt)

---

## 10. Localization (i18n)

> Full spec: `docs/localization.md`

### Supported Locales
`en` (default), `es`, `fr-CA`, `de`, `nl`, `pt`, `ru`, `hi`, `zh`, `ja`, `ko`, `ar`, `he`

RTL locales: `ar`, `he`. Translation files for all 13 locales are created upfront; RTL layout work ships in a separate story.

### Web (FastAPI + Jinja2)
- Python `babel` + Jinja2 `i18n` extension (gettext)
- Strings in templates: `{{ _("Save office") }}` / `{% trans %}Save office{% endtrans %}`
- Translation files: `src/locales/{lang}/LC_MESSAGES/messages.po` (compiled to `.mo`)
- Extract: `pybabel extract -F babel.cfg -o src/locales/messages.pot src/`
- Locale detection order: session cookie `lang` → `Accept-Language` header → `en`
- Language switcher: icon button in top app bar

### Mobile (React Native / Expo — future)
- Same pattern as `gaming_app` and `book_app`: `i18next` + `react-i18next` + `i18next-resources-to-backend`
- `expo-localization` for device locale; same `resolveLocale()` fallback logic
- Namespaces mirror web namespaces; `en` bundled statically, others lazy-loaded
- `useHtmlAttributes()` hook sets `<html lang>` and `<html dir>` (same pattern as `gaming_app`)

### Namespaces
| Namespace | Contents |
|---|---|
| `common` | Shared UI: buttons, labels, errors, confirmations, date formats |
| `nav` | Sidebar and top-bar navigation labels |
| `offices` | Office list + edit form: field labels, placeholders, validation |
| `run` | Scraper / Command Center: run modes, options, progress labels |
| `research` | Gemini research, Wiki drafts |
| `refs` | Reference data labels |
| `auth` | Login screen |
| `system` | Scheduled jobs, settings, system admin |

### Meta Files
Each namespace has `src/locales/_meta/{namespace}.meta.json` with per-key:
- `description` — what the string is and where it appears
- `tone` — neutral / action / functional / instructional / celebratory
- `characterLimit` — max characters the UI can display
- `placeholders` — list of interpolation variables (e.g. `["{{name}}"]`)
- `doNotTranslate` — proper nouns to keep in source language (e.g. `["Wikipedia", "RulersAI"]`)

### RTL Rules
- `<html dir="rtl">` set server-side for `ar` and `he` locales
- All layout uses CSS logical properties: `margin-inline-start`, `padding-inline-end`, `border-inline-start`, etc.
- Sidebar: flips to right side in RTL (`right: 0`, main content uses `margin-inline-start: 16rem`)
- Floating outline sidebar (multi-office editor): flips to opposite side
- Directional icons (chevrons, arrows): `[dir=rtl] .icon-directional { transform: scaleX(-1); }`
- Font stacks: Arabic → `'Inter', 'Noto Sans Arabic', sans-serif`; Hebrew → `'Inter', 'Noto Sans Hebrew', sans-serif`
- RTL is gated: `ar`/`he` files are created in i18n story but layout ships in a separate RTL story

---

## 11. Accessibility (WCAG 2.2 A + AA — ground-up implementation)

The app has no existing WCAG compliance. Every screen story ships with a11y baked in.
This section is the authoritative spec for all screen stories (5–14).

### Approach
- **Infrastructure** (STORY-16a): skip link, audio mute, focus-ring CSS, `aria-live` containers, axe CI — lives in `base.html` and `theme.css`
- **Per-screen** (stories 5–14): each screen story includes a11y as part of its definition of done
- **Audit** (STORY-16b): after all screens ship — keyboard walkthrough + screen reader testing + fix stragglers

### What is N/A for this app
- 1.2.x — no video or prerecorded audio content
- 2.1.4 — no single-character keyboard shortcuts
- 2.2.1 — no session timeouts affecting users
- 2.3.1 — no flashing content
- 2.5.1 — no multi-point pointer gestures
- 3.3.7 Redundant Entry — no multi-step form flows requiring re-entry
- 3.3.8 Accessible Authentication — Google OAuth, no CAPTCHA ✓

---

### Infrastructure (STORY-16a — part of STORY-3/4)

**Skip link** — first child of `<body>`:
```html
<a href="#main-content"
   class="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4 focus:z-50
          focus:bg-primary-container focus:text-white focus:px-4 focus:py-2 focus:rounded">
  Skip to main content
</a>
```

**Landmarks:**
```html
<nav aria-label="Primary navigation">   <!-- sidebar -->
<header role="banner">                   <!-- top app bar -->
<main id="main-content">                 <!-- page content -->
```

**Focus ring** — single CSS rule in `theme.css` covers the whole app:
```css
*:focus-visible {
  outline: 2px solid var(--color-primary);
  outline-offset: 2px;
}
*:focus:not(:focus-visible) {
  outline: none;  /* suppress on mouse click, keep on keyboard */
}
```

**`scroll-padding-top`** (2.4.11 Focus Not Obscured):
```css
html { scroll-padding-top: 4.5rem; }  /* equals top bar height */
```

**`aria-live` regions** — in `base.html`, before `</body>`. JS writes to these; never updates arbitrary divs:
```html
<div id="live-polite"   aria-live="polite"   aria-atomic="true"  class="sr-only"></div>
<div id="live-assertive" aria-live="assertive" aria-atomic="true" class="sr-only"></div>
```
Usage: `document.getElementById('live-polite').textContent = 'Saved.'`

**Audio mute toggle** (1.4.2 Audio Control):
The job completion sound plays 5 tones over ~4.5 seconds — exceeds the 3-second threshold.
Add a `🔇` icon button to the top bar that toggles `window._soundMuted`.
`playJobCompleteSound()` checks `if (window._soundMuted) return;` before playing.
Persist preference in `localStorage`.

---

### Per-Screen Requirements (definition of done for stories 5–14)

**Page titles (2.4.2):**
Every page has a unique, descriptive title:
```html
{% block title %}Edit Office — RulersAI{% endblock %}
{% block title %}Offices — RulersAI{% endblock %}
{% block title %}Command Center — RulersAI{% endblock %}
```
Format: `[Page Name] — RulersAI`

**Form labels (1.3.1):**
- Every `<input>`, `<select>`, `<textarea>` has an associated `<label for="id">` or is wrapped inside a `<label>`
- Wrapping is preferred for checkboxes; `for`/`id` is required for all other inputs
- No orphaned `<label>Text</label><input>` without explicit association

**Required fields (3.3.2):**
```html
<label for="office-name">
  Office name
  <span aria-hidden="true"> *</span>
  <span class="sr-only">(required)</span>
</label>
<input id="office-name" aria-required="true" ...>
```

**Validation errors (3.3.1, 3.3.3):**
```html
<!-- Error banner -->
<div role="alert">
  <strong>Save failed</strong> — Office name is required. Enter a name and try again.
</div>
<!-- Failing field -->
<input id="office-name" aria-invalid="true" aria-describedby="office-name-error">
<span id="office-name-error" class="field-error">Office name is required.</span>
```
Error messages must say both what failed AND how to fix it (3.3.3).

**Heading hierarchy:**
- One `<h1>` per page — the masthead title
- Card headers: `<h2>`
- Sub-sections within cards: `<h3>`
- Table config blocks: `<h4>`
- Never skip levels

**Data tables (1.3.1):**
```html
<table aria-label="Offices">
  <thead>
    <tr>
      <th scope="col" aria-sort="none">Name</th>
      <th scope="col">Country</th>
    </tr>
  </thead>
```
Update `aria-sort="ascending/descending"` via JS when user sorts a column.

**Custom toggles (4.1.2):**
```html
<button role="switch" aria-checked="true" class="toggle">
  <span class="sr-only">Include in runs: </span>
  <span aria-hidden="true"><!-- visual toggle --></span>
</button>
```

**Progress bars (4.1.2):**
```html
<div role="progressbar"
     aria-valuenow="42" aria-valuemin="0" aria-valuemax="100"
     aria-label="Offices: 42 of 120"
     aria-valuetext="42 of 120 offices">
  <!-- visual bar -->
</div>
```
Update `aria-valuenow` and `aria-valuetext` via JS on each poll cycle.
Also write a summary to the `aria-live="polite"` region on completion.

**Collapsible panels (4.1.2):**
```html
<button aria-expanded="false" aria-controls="preview-panel-id">
  Preview
</button>
<div id="preview-panel-id" hidden>...</div>
```
When panel opens: `hidden` removed, `aria-expanded="true"`, focus moved to panel heading or first interactive element.

**Icon buttons (2.4.4):**
```html
<button aria-label="Test config for US House of Representatives">
  <span class="material-symbols-outlined" aria-hidden="true">science</span>
</button>
```
Label must name the action AND the target.

**Input purpose (1.3.5):**
```html
<input autocomplete="url">         <!-- URL fields -->
<input autocomplete="name">        <!-- name fields -->
<input autocomplete="off">         <!-- table numbers, column indices -->
```

**Target size (2.5.8):**
- All interactive elements: minimum 24×24 CSS px hit area
- Mobile touch targets: minimum 44×44px
- Icon buttons in toolbars: use `p-1.5` minimum (gives 24px)
- Toggle switch thumb: minimum 24px

**Focus not obscured (2.4.11):**
- `scroll-padding-top` in base CSS handles the sticky top bar
- Floating outline sidebar: only contains jump links, not form elements — no overlap concern
- Sticky save-all bar: positioned at top (below top app bar), not bottom — test that elements at the bottom of the page aren't obscured

**Consistent help (3.2.6):**
- "Support" link in sidebar footer, same position on every page
- `ⓘ` info icons on fields use `<button type="button" aria-label="Help: [field name]">` opening a tooltip or small description block — not bare `title` attributes (inaccessible on touch/keyboard)

**Color not sole indicator (1.4.1):**
- Status badges: text label always present alongside color (`Enabled` / `Disabled`)
- Error states: `aria-invalid` + red border + error text — never red border alone

**Input border contrast (1.4.11):**
Use `outline` (#74777d) for input borders — 4.6:1 against white. Never `outline-variant`.

---

### Testing (STORY-16b)
- `@axe-core/playwright` — one test per key page, fails CI on any axe violation
- Keyboard-only walkthrough of three flows after all screen stories ship:
  1. Create a new office and save
  2. Run scraper, monitor progress, cancel job
  3. Review wiki draft and update status
- Screen reader testing: NVDA + Chrome (Windows), VoiceOver + Safari (macOS)
- Confirm: job completion sound can be muted, status messages are announced, error fields are announced on save failure

---

## 12. Story Map

| # | Story | Key deliverables | Depends on |
|---|---|---|---|
| 1 | App Icon & Favicon (PR #442) | Icon asset, favicon, apple-touch-icon | — |
| 2 | Branding Guidelines (this file) | Color tokens, typography, component spec, a11y spec, i18n spec | — |
| 3 | Design Tokens & Global CSS | `theme.css` rewrite, Inter + Material Symbols, CSS focus ring, `scroll-padding-top` | — |
| **16a** | **A11y Infrastructure** | Skip link, `aria-live` regions, audio mute toggle, axe CI setup — all in `base.html` / `theme.css` | 3 |
| 4 | Layout Shell | Fixed sidebar, top bar, `<main id>`, landmarks, dark mode toggle, audio mute button | 3, 16a |
| 5 | Office List Page | Masthead, restyled table with `<th scope>` + `aria-sort`, filter bar, status badges | 4 |
| 6 | Edit Office — Single Mode | Cards, bento layout, all form labels associated, progress bars with ARIA, error field markup | 4 |
| 7 | Edit Office — Multi-Office Mode | Collapsible office sections with `aria-expanded`, save-all bar, restyled floating outline | 6 |
| 8 | Scraper / Command Center | Run mode tiles, options toggles as `role="switch"`, progress bars with ARIA, cancel keyboard accessible | 4 |
| 9 | Wiki Drafts | Status tabs, draft cards, detail split-pane | 4 |
| 10 | Gemini Research | Search card, research progress panel | 4 |
| 11 | Login Page | Full-page card, Google OAuth button | 3 |
| 12 | Remaining Pages (bulk pass) | Apply shell + tokens to reports, refs, system admin, data views | 4 |
| 13 | Dark Mode (end-to-end) | Toggle persistence, verify all pages in dark | 4 |
| 14 | Mobile Layouts | Drawer, bottom nav, table→card, edit-office 3-tier | 4, 6 |
| 15a | i18n Foundation | `babel` setup, en strings extracted to `.po`, 12 locale stubs, language switcher in top bar | 4 |
| 15b | RTL Layout | `ar` + `he` layout — logical CSS properties, sidebar flip, directional icon flip, Noto font fallbacks | 15a |
| 15c | Translation Delivery | All 13 locales translated, meta files, CI locale smoke tests | 15a |
| **16b** | **A11y Audit** | axe on all pages, keyboard walkthroughs (3 flows), NVDA + VoiceOver testing, fix stragglers | 5–14 |

**Parallel tracks after STORY-4:** Stories 5–13 can run in parallel. Stories 14, 15a, and 16b are independent of each other after their stated dependencies.
