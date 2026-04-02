# -*- coding: utf-8 -*-
"""Seed the reference_documents table with Wikipedia formatting guidelines."""

from __future__ import annotations

from .connection import get_connection
from .reference_documents import get_reference_document, upsert_reference_document

# Condensed Wikipedia Manual of Style guidelines relevant to biographical articles
# about political office holders. This is stored in the DB so the OpenAI polish
# step can reference it without fetching from Wikipedia each time.
_WIKIPEDIA_MOS_CONTENT = """\
## Wikipedia Manual of Style — Biographical Articles (Condensed)

### Article Structure
1. **Lead section**: One to four paragraphs summarizing the subject. Include full name, \
birth/death dates, nationality, and why they are notable. Do not use references in the \
lead if the information is sourced in the body.
2. **Infobox**: Use {{Infobox officeholder}} for political figures. Include: name, image, \
office, term_start, term_end, predecessor, successor, birth_date, birth_place, death_date, \
death_place, party, spouse, children, alma_mater.
3. **Body sections** (in order): Early life and education, Career (or Political career), \
Personal life, Death/Legacy, See also, References, External links.

### Formatting Rules
- **Dates**: Use {{birth date|YYYY|MM|DD}} and {{death date and age|YYYY|MM|DD|YYYY|MM|DD}}.
- **References**: Use inline <ref> tags. Group with {{reflist}} in ==References== section.
- **Categories**: Add [[Category:Year births]], [[Category:Year deaths]], \
[[Category:Office holders by country/state]].
- **Links**: First mention of notable people, places, and offices should be wikilinked.
- **Neutral point of view**: No promotional or biased language.
- **Verifiability**: Every claim must be attributable to a reliable source.

### Infobox Template
{{Infobox officeholder
| name          =
| image         =
| office        =
| term_start    =
| term_end      =
| predecessor   =
| successor     =
| birth_date    = {{birth date|YYYY|MM|DD}}
| birth_place   =
| death_date    = {{death date and age|YYYY|MM|DD|YYYY|MM|DD}}
| death_place   =
| party         =
| spouse        =
| children      =
| alma_mater    =
}}

### Reference Format
<ref name="source_name">{{cite web |url= |title= |publisher= |date= |access-date=}}</ref>

### Stub Articles
If insufficient information, add {{politician-stub}} or {{US-politician-stub}} at the \
bottom, before categories.
"""


def seed_wikipedia_mos(conn=None) -> None:
    """Seed or update the Wikipedia Manual of Style reference document."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        upsert_reference_document("wikipedia_mos", _WIKIPEDIA_MOS_CONTENT, conn=conn)
        if own_conn:
            conn.commit()
    finally:
        if own_conn:
            conn.close()
