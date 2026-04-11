# RulersAI — Accessibility (WCAG 2.2 A + AA)

This document is the engineering specification for WCAG 2.2 Level A and Level AA compliance across all RulersAI surfaces (web and mobile). The app had no prior WCAG implementation; this spec covers ground-up compliance.

For visual design tokens, color contrast values, and component patterns see `docs/branding.md`.  
For locale-specific considerations (lang/dir attributes) see `docs/localization.md`.

---

## Baseline Audit (pre-implementation)

### What existed before this work

| Item | Status |
|---|---|
| `<html lang="en">` (static) | ✓ |
| `<nav>` and `<main>` elements | ✓ partial — unlabelled, `<main>` has no `id` |
| `role="alert"` on validation error banner | ✓ in page_form only |
| `aria-label` on ~5 specific buttons | ✓ sparse |
| `aria-hidden="true"` on loading spinner | ✓ |
| CSS focus ring | ✗ none — browser defaults only |
| Skip link | ✗ |
| `aria-live` regions | ✗ zero instances |
| `aria-required` / `aria-invalid` | ✗ zero instances |
| `aria-expanded` / `aria-controls` | ✗ zero instances |
| `role="progressbar"` | ✗ zero instances |
| `role="switch"` | ✗ zero instances |
| `autocomplete` attributes | ✗ zero instances |
| `<th scope>` on data tables | ✗ none |
| Audio mute control | ✗ (completion sound plays ~4.5s uncontrolled) |

### Confirmed Level A failures (pre-implementation)

| # | Criterion | Failure |
|---|---|---|
| 1 | **1.3.1 Info and Relationships** | Many `<label>Text</label><input>` pairs in `page_form.html` and `run.html` have no `for`/`id` linkage — screen readers do not announce field names |
| 2 | **1.4.2 Audio Control** | Job completion sound plays 5 tones over ~4.5 seconds (exceeds 3-second limit). No mute or stop control |
| 3 | **2.4.1 Bypass Blocks** | No skip link — keyboard users tab through the full nav on every page |
| 4 | **2.4.2 Page Titled** | All pages share the same generic title; no page-specific context |
| 5 | **2.4.3 Focus Order** | Sortable table columns have no keyboard interaction. Dynamically-opened preview panels receive no focus |
| 6 | **4.1.2 Name, Role, Value** | Progress bars are plain `<div>` elements. Job status text updated by JS has no `aria-live`. Sortable `<th>` elements have no `aria-sort` |

### Confirmed Level AA failures (pre-implementation)

| # | Criterion | Failure |
|---|---|---|
| 7 | **1.3.5 Identify Input Purpose** | No `autocomplete` on any input |
| 8 | **1.4.11 Non-text Contrast** | Input borders use `outline-variant` (#c4c6cd) — fails 3:1 against white |
| 9 | **2.4.6 Headings and Labels** | Inconsistent heading hierarchy across templates; some pages skip levels |
| 10 | **2.4.7 Focus Visible** | No CSS focus ring defined — browser defaults unreliable, especially Chrome |
| 11 | **3.3.1 Error Identification** | Validation error banner does not associate to the failing field — no `aria-invalid`, no `aria-describedby` |
| 12 | **3.3.3 Error Suggestion** | Error messages state what failed but give no guidance on how to fix |
| 13 | **4.1.3 Status Messages** | "Saved." alerts, job progress updates, and completion have no `aria-live` — screen readers never hear them |

---

## Criteria Not Applicable to This App

| Criterion | Reason |
|---|---|
| 1.2.x (captions, audio description) | No video or prerecorded audio content |
| 2.1.4 Character Key Shortcuts | No single-character keyboard shortcuts defined |
| 2.2.1 Timing Adjustable | No session timeouts affecting users |
| 2.3.1 Three Flashes | No flashing content |
| 2.5.1 Pointer Gestures | No multi-point pointer gestures required |
| 3.3.7 Redundant Entry | No multi-step forms requiring re-entry of data |
| 3.3.8 Accessible Authentication | Google OAuth — no CAPTCHA or cognitive test ✓ |

---

## Implementation Strategy

Accessibility is **baked into every screen story**, not bolted on at the end.

- **STORY-16a (Infrastructure):** Shell-level items that all screens inherit — lives in `base.html` and `theme.css`. Do alongside design token work.
- **Stories 5–14 (Screen stories):** Each screen story has the per-screen requirements below as acceptance criteria.
- **STORY-16b (Audit):** After all screens ship — automated + manual testing, fix stragglers.

---

## Infrastructure (STORY-16a)

Everything in this section lives in `base.html` or `theme.css` and applies globally.

### Skip Link

First child of `<body>`:

```html
<a href="#main-content"
   class="sr-only focus:not-sr-only focus:absolute focus:top-4 focus:left-4
          focus:z-50 focus:bg-primary-container focus:text-white
          focus:px-4 focus:py-2 focus:rounded focus:text-sm focus:font-bold">
  Skip to main content
</a>
```

Satisfies: **2.4.1 Bypass Blocks**

### Landmarks

```html
<header role="banner">                          <!-- top app bar -->
<nav aria-label="Primary navigation">           <!-- sidebar -->
<main id="main-content">                        <!-- page content -->
```

The `id="main-content"` is the skip link target. Every page uses `<main>` via `base.html`.

### Focus Ring (theme.css)

```css
/* Keyboard focus: visible ring */
*:focus-visible {
  outline: 2px solid var(--color-primary);
  outline-offset: 2px;
}

/* Mouse click: no outline (browser default suppressed) */
*:focus:not(:focus-visible) {
  outline: none;
}
```

Satisfies: **2.4.7 Focus Visible**

### Scroll Padding (theme.css)

```css
html {
  scroll-padding-top: 4.5rem; /* equals sticky top bar height */
}
```

Satisfies: **2.4.11 Focus Not Obscured (Minimum)** — prevents the sticky top bar from hiding a focused element when the user scrolls via Tab.

### aria-live Regions

Two regions at the end of `<body>`, before `</body>`:

```html
<div id="live-polite"
     aria-live="polite"
     aria-atomic="true"
     class="sr-only"
     aria-label="Status messages"></div>

<div id="live-assertive"
     aria-live="assertive"
     aria-atomic="true"
     class="sr-only"
     aria-label="Urgent notifications"></div>
```

**Global JS helper** (in `base.html`):

```js
window.announce = function(message, priority = 'polite') {
  const region = document.getElementById(
    priority === 'assertive' ? 'live-assertive' : 'live-polite'
  );
  if (!region) return;
  // Clear first so repeated identical messages still fire
  region.textContent = '';
  requestAnimationFrame(() => { region.textContent = message; });
};
```

**Contract:** All JS that produces a user-visible status message (save confirmation, job progress, job completion, error notification) must call `window.announce()` instead of — or in addition to — updating a visible element. Never write status to an element without an `aria-live` attribute.

Satisfies: **4.1.3 Status Messages**

### Audio Mute Toggle

The job completion sound (`playJobCompleteSound`) plays 5 tones over ~4.5 seconds — exceeds the 3-second threshold for **1.4.2 Audio Control**.

**Fix:**

```js
// In base.html, update playJobCompleteSound:
window.playJobCompleteSound = function() {
  if (window._soundMuted) return;  // ADD THIS LINE
  // ... rest of existing function
};

// Mute toggle wired to a button in the top bar:
window._soundMuted = localStorage.getItem('rulersai_sound_muted') === 'true';

window.toggleSound = function() {
  window._soundMuted = !window._soundMuted;
  localStorage.setItem('rulersai_sound_muted', window._soundMuted);
  const btn = document.getElementById('soundToggleBtn');
  if (btn) {
    btn.setAttribute('aria-label', window._soundMuted
      ? 'Unmute completion sound'
      : 'Mute completion sound');
    btn.setAttribute('aria-pressed', window._soundMuted);
  }
};
```

Button in top bar:

```html
<button id="soundToggleBtn"
        type="button"
        onclick="toggleSound()"
        aria-label="Mute completion sound"
        aria-pressed="false"
        class="icon-btn">
  <span class="material-symbols-outlined" aria-hidden="true">volume_up</span>
</button>
```

Satisfies: **1.4.2 Audio Control**

### axe-core CI Setup

```bash
npm install --save-dev @axe-core/playwright
```

Test pattern (add to existing Playwright suite):

```ts
import AxeBuilder from '@axe-core/playwright';

test('offices list has no axe violations', async ({ page }) => {
  await page.goto('/offices');
  const results = await new AxeBuilder({ page })
    .withTags(['wcag2a', 'wcag2aa', 'wcag22aa'])
    .analyze();
  expect(results.violations).toEqual([]);
});
```

One test per key page. Fails CI on any violation. Add to: `/offices`, `/offices/new`, `/offices/{id}`, `/run`, `/data/wiki-drafts`, `/gemini-research`, `/login`.

---

## Per-Screen Requirements

These are acceptance criteria for **every** screen story (stories 5–14). A story is not done until all of these pass.

### Page Titles (2.4.2)

Every page has a unique, descriptive `<title>`. Format: `[Page Name] — RulersAI`.

```
Offices — RulersAI
Edit Office — RulersAI
New Office — RulersAI
Command Center — RulersAI
Research Output — RulersAI
Deep Research — RulersAI
Sign in — RulersAI
Reports — RulersAI
Reference Data — RulersAI
```

### Heading Hierarchy (2.4.6)

- One `<h1>` per page — the masthead title
- Card/section headers: `<h2>`
- Sub-sections within cards: `<h3>`
- Table config blocks within office sections: `<h4>`
- Never skip levels (no `<h1>` → `<h3>`)

### Form Label Association (1.3.1)

Every `<input>`, `<select>`, and `<textarea>` must be programmatically associated to its label.

**Preferred pattern (wrapping — for checkboxes and radios):**
```html
<label>
  <input type="checkbox" name="enabled" value="1">
  Include in runs
</label>
```

**Required pattern (explicit for — for all other inputs):**
```html
<label for="office-name">Office name</label>
<input id="office-name" type="text" name="name">
```

No `<label>Text</label><input>` without either wrapping or `for`/`id` pairing. This was the most widespread failure in the pre-implementation audit.

### Required Fields (3.3.2, 4.1.2)

```html
<label for="office-name">
  Office name
  <span aria-hidden="true"> *</span>
  <span class="sr-only">(required)</span>
</label>
<input id="office-name"
       type="text"
       name="name"
       aria-required="true"
       required>
```

### Validation Errors (3.3.1, 3.3.3)

Error banner: `role="alert"` so screen readers announce immediately.
Failing field: `aria-invalid="true"` + `aria-describedby` pointing to error message element.
Error text: must state both what failed AND how to fix it.

```html
<!-- Error banner -->
<div role="alert" class="alert alert-error">
  <strong>Save failed</strong> — Office name is required.
  Enter a name for this office and try again.
</div>

<!-- Failing field -->
<label for="office-name">Office name <span aria-hidden="true">*</span></label>
<input id="office-name"
       aria-invalid="true"
       aria-describedby="office-name-error"
       aria-required="true">
<span id="office-name-error" class="field-error" role="none">
  Office name is required.
</span>
```

### Input Purpose (1.3.5)

Apply `autocomplete` to all user-facing inputs:

| Input type | `autocomplete` value |
|---|---|
| Wikipedia URL | `url` |
| Office name, department | `off` (unique institutional names) |
| Table numbers, column indices | `off` |
| Search fields | `off` |
| Login email (if ever shown) | `email` |

### Data Tables (1.3.1)

```html
<table aria-label="Offices">
  <caption class="sr-only">List of configured offices with status and actions</caption>
  <thead>
    <tr>
      <th scope="col">Enabled</th>
      <th scope="col" aria-sort="none">
        <button type="button" class="sort-btn" aria-label="Sort by name">
          Name <span class="sort-indicator" aria-hidden="true"></span>
        </button>
      </th>
      <th scope="col">Country</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><button role="switch" aria-checked="true" ...>...</button></td>
      <td>US Senate</td>
      <td>United States</td>
    </tr>
  </tbody>
</table>
```

When user clicks a sortable column header, update `aria-sort` on that `<th>`:
- `aria-sort="ascending"` / `aria-sort="descending"` on the active sort column
- `aria-sort="none"` on all other sortable columns

Satisfies: **1.3.1**, **4.1.2**

### Custom Toggle Switches (4.1.2)

```html
<button type="button"
        role="switch"
        aria-checked="true"
        class="toggle-switch"
        aria-label="Include US Senate in scraper runs">
  <span aria-hidden="true" class="toggle-thumb"></span>
  <span class="sr-only">Include in runs</span>
</button>
```

When toggled, update `aria-checked` and call `window.announce('US Senate enabled')`.

### Progress Bars (4.1.2)

```html
<div role="progressbar"
     id="office-progress-bar"
     aria-valuenow="42"
     aria-valuemin="0"
     aria-valuemax="120"
     aria-label="Scraping offices"
     aria-valuetext="42 of 120 offices">
  <div class="progress-bar-fill" style="width: 35%"></div>
</div>
<div class="progress-label" aria-hidden="true">Offices: 42 / 120</div>
```

JS update on each poll cycle:
```js
bar.setAttribute('aria-valuenow', current);
bar.setAttribute('aria-valuetext', `${current} of ${total} offices`);
// On completion only, announce to live region:
if (current >= total) window.announce(`Offices complete: ${total} processed`);
```

### Collapsible Sections (4.1.2)

```html
<button type="button"
        aria-expanded="false"
        aria-controls="table-config-panel-1"
        class="collapse-trigger">
  Table Extraction Config
  <span class="material-symbols-outlined" aria-hidden="true">expand_more</span>
</button>
<div id="table-config-panel-1" hidden>
  <!-- fields -->
</div>
```

When opened: remove `hidden`, set `aria-expanded="true"`, move focus to the panel's first interactive element or heading.

### Icon-Only Buttons (2.4.4, 4.1.2)

```html
<button type="button"
        aria-label="Test config for US Senate table 1"
        class="icon-btn">
  <span class="material-symbols-outlined" aria-hidden="true">science</span>
</button>
```

The `aria-label` must name the **action** and the **target** (not just the action).  
Material Symbols icons must always have `aria-hidden="true"` — they are decorative.

### Tooltips on Info Icons (3.2.6)

Do not use `title` attribute for help text — inaccessible on keyboard and touch.

```html
<button type="button"
        class="info-icon-btn"
        aria-label="Help: Infobox role key"
        aria-expanded="false"
        aria-controls="infobox-help-text">
  <span class="material-symbols-outlined" aria-hidden="true">info</span>
</button>
<div id="infobox-help-text"
     role="tooltip"
     hidden>
  Use exact role phrase to disambiguate shared links. Example: "chief judge".
</div>
```

### Target Size (2.5.8 — new in WCAG 2.2)

Minimum 24×24 CSS px for all interactive elements (AA requirement).  
Recommended 44×44px for mobile touch targets.

- Icon buttons in toolbars: `min-width: 24px; min-height: 24px; padding: 0.375rem` (p-1.5)
- Toggle switch thumb: minimum 24px × 24px
- Sortable column buttons: full `<th>` width, `padding: 0.5rem`

### Focus Not Obscured (2.4.11 — new in WCAG 2.2)

The `scroll-padding-top: 4.5rem` in `theme.css` handles the sticky top bar.

Additional check: the floating outline sidebar (multi-office editor) must not cover any focusable form element. The sidebar occupies `right: 0`, and the form content uses `margin-right: 13rem` — verify these values do not create overlap on viewports 1024px–1280px wide.

### Status Messages (4.1.3)

All of these must call `window.announce()`:
- Save confirmation: `window.announce('Office saved successfully.')`
- Job started: `window.announce('Scraper job started.')`
- Job cancelled: `window.announce('Job cancelled.')`
- Job completed: `window.announce('Scraper job complete. Processed 1,284 offices.')`
- Preview complete: `window.announce('Preview complete. 42 results found.')`
- Error: `window.announce('Save failed. See errors above.', 'assertive')`

Visible status text elements are still updated visually — `announce()` is additive, not a replacement.

### Dragging Alternative (2.5.7 — new in WCAG 2.2)

The "Move table" drag-to-reorder has a keyboard-accessible alternative: the "Move to office" `<select>` + "Move table" `<button>`. These must always be visible and keyboard-reachable alongside any drag handle. This is already the current implementation — ensure it is not removed during the redesign.

### Color Not Sole Indicator (1.4.1)

Status badges must always include text alongside color:
- ✓ `<span class="badge badge-enabled">Enabled</span>` — text + green background ✓
- ✗ `<span class="badge badge-enabled"></span>` — green background only ✗

Error states: `aria-invalid` + red border + error text below field. Never red border alone.

### Input Border Contrast (1.4.11)

Input, select, and textarea borders: use `--color-outline` (#74777d), which achieves 4.6:1 against white — passes 3:1 threshold for non-text contrast.  
Never use `--color-outline-variant` (#c4c6cd) for borders — fails 3:1.

---

## Mobile Accessibility

Mobile-specific additions on top of all web requirements:

- Minimum touch target size: **44×44px** (exceeds the 24px AA minimum; recommended for mobile)
- Hamburger/menu button: `aria-label="Open navigation"` / `"Close navigation"`, `aria-expanded` reflecting drawer state
- Navigation drawer when open: `role="dialog" aria-modal="true" aria-label="Navigation"` with focus trap (Tab cycles within drawer; Escape closes and returns focus to hamburger button)
- Bottom navigation bar: `role="navigation" aria-label="Primary mobile navigation"`; active item has `aria-current="page"`
- `prefers-reduced-motion`: wrap all CSS transitions and animations in `@media (prefers-reduced-motion: no-preference)` — default state is no animation

---

## Testing Protocol

### Automated (CI)

`@axe-core/playwright` runs on every Playwright CI execution. Tags: `wcag2a`, `wcag2aa`, `wcag22aa`.

Pages with required axe tests:
- `/login`
- `/offices`
- `/offices/new`
- `/offices/{id}` (single-office mode)
- `/offices/{id}` (multi-office mode — test against a page with 2+ offices)
- `/run`
- `/data/wiki-drafts`
- `/data/wiki-drafts/{id}`
- `/gemini-research`
- `/refs`
- `/data/ai-decisions`
- `/data/individuals`

### Manual Keyboard Walkthroughs (STORY-16b)

Three flows, each verified keyboard-only (Tab, Shift+Tab, Enter, Space, Escape, arrow keys):

**Flow 1: Create office**
1. Navigate to `/offices/new` via skip link + sidebar
2. Tab through all fields in Source Context card
3. Tab through Office Details card
4. Tab through Table Extraction Config (expand with Enter on trigger)
5. Submit — verify error announced if validation fails
6. Fix error — verify correct field is focused/announced
7. Submit successfully — verify "Office saved" announced in live region

**Flow 2: Run scraper and cancel**
1. Navigate to `/run` via sidebar
2. Select run mode using keyboard (tile grid or select)
3. Set options using keyboard
4. Start job — verify "Job started" announced
5. Monitor progress bars — verify `aria-valuenow` updates
6. Cancel job with keyboard — verify "Job cancelled" announced

**Flow 3: Review wiki draft**
1. Navigate to `/data/wiki-drafts`
2. Filter by status using keyboard
3. Open a draft detail page
4. Tab through the split-pane (wikitext + preview)
5. Update status using keyboard — verify status change announced

### Screen Reader Testing (STORY-16b)

| SR + Browser | Platform | Flows to test |
|---|---|---|
| NVDA 2024 + Chrome | Windows | All 3 flows |
| VoiceOver + Safari | macOS | All 3 flows |

For each flow verify:
- Field names announced when focused
- Required fields announced as required
- Error messages announced on validation failure
- Status messages (save, progress, completion) announced without focus change
- Custom controls (toggles, progress bars) announce role, state, and value

### Color Contrast Audit

Run once before STORY-16b closes:
- Browser DevTools → Accessibility panel → check every text color / background pair on: offices list, edit office form, login page, run page
- Minimum 4.5:1 for normal text, 3:1 for large text (≥18pt / ≥14pt bold) and UI components
