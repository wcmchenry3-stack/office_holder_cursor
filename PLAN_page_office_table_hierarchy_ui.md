# Page → Office → Table hierarchy in the form UI

## Goal
Revise `src/templates/page_form.html` so that:
- **Page**-level options appear under a "Page" section.
- **Office**-level options appear under an "Office" section.
- **Table**-level options appear under the existing "Table" section.

No backend or form field names change; only the order and grouping of existing fields and the section headings.

---

## Current structure (no hierarchy sections)
- Banner (optional), then h1 "Office", then form with:
  - Country, State | Level, Branch
  - Department, Office name
  - Include in runs, URL, Notes
  - **h2 Table parsing** → table no/rows, columns, checkboxes, date source, rowspan options, alt links, include main
  - Actions

---

## Target structure

### 1. Page (new section)
**Heading:** Use a clear section label, e.g. **h2 "Page"** with optional short note: "Source page (URL and location)."

**Fields under Page:**
- **URL** (the Wikipedia page URL)
- **Country**, **State / Province / Territory**
- **Level**, **Branch**

Rationale: The page is the source we scrape; URL and location (country, state, level, branch) describe that source.

### 2. Office (new section)
**Heading:** **h2 "Office"** with optional note: "Office definition on this page."

**Fields under Office:**
- **Department**
- **Office name**
- **Include in runs** (checkbox)
- **Notes**

Rationale: These identify and describe the office, not the table or the raw page.

### 3. Table (existing section)
**Heading:** Keep **h2 "Table parsing"** (or **h2 "Table"**). Optional note: "Which table and how to parse it."

**Fields under Table (unchanged grouping, already correct):**
- Table no, Table rows (min)
- Link column, Party column, Term start/end, District column
- Term dates merged, Ignore party, District: Ignore / At-Large
- Dynamic parse, Read right to left, Date source, Parse rowspan, Consolidate rowspan terms, Rep link, Party link, Use full page for table fetch
- **Alt links** and **Include main office link in search** (used for Find date in infobox; stay with table parsing)

Actions (Save, Save and close, Cancel, Test config, etc.) stay at the bottom of the form.

---

## Document outline (headings)
- **h1** Office (existing; keep as main title)
- **h2** Page
- **h2** Office
- **h2** Table parsing

Preview/panels (Preview, All tables, etc.) keep their existing h2s; they are not part of the form hierarchy.

---

## Implementation steps

1. **In `src/templates/page_form.html`:**
   - After the form opens and any nav/alerts, add **h2 "Page"** and a short form-note if desired.
   - Move **URL** to the top of the Page section.
   - Keep **Country, State** and **Level, Branch** in the same rows but directly under Page (they are already consecutive; just ensure they are the only fields between the "Page" h2 and the next section).
   - Add **h2 "Office"** (and optional form-note).
   - Group **Department**, **Office name**, **Include in runs**, **Notes** under Office (in current order).
   - Leave **h2 "Table parsing"** and all table fields (from "Table no" through "Include main office link in search") as the Table section.

2. **Optional:** Remove or relax the debug banner ("Form: page_form ...") if you no longer need it, or leave it for now.

3. **No changes** to `src/main.py` or any backend: same form action, same field names, same POST handling.

---

## Field move summary

| Field(s)           | Current position     | New section |
|--------------------|----------------------|-------------|
| Country, State     | Top of form          | Page        |
| Level, Branch      | After Country/State  | Page        |
| Department, Name   | After Level/Branch   | Office      |
| Include in runs    | After Name           | Office      |
| URL                | After Include in runs| **Page** (move up) |
| Notes              | After URL            | Office      |
| Table parsing h2   | After Notes          | (unchanged) Table |
| All table fields   | After h2             | Table       |

So the only structural change is: **move URL to the top of the form** and insert the two new headings "Page" and "Office" so that:
- **Page** contains: URL, Country, State, Level, Branch.
- **Office** contains: Department, Office name, Include in runs, Notes.
- **Table** contains: everything from "Table no" through "Include main office link in search".

This keeps the hierarchy clear and ensures "the appropriate options are listed under the appropriate hierarchy" without changing behavior or backend.
