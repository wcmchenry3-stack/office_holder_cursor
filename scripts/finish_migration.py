# -*- coding: utf-8 -*-
"""Finish the partial SQLite→Postgres migration, skipping orphaned FK rows."""

import os
import sqlite3
import psycopg2

sq = sqlite3.connect("/data/office_holder.db")
pg = psycopg2.connect(os.environ["DATABASE_URL"])
cur = pg.cursor()


def pg_ids(table):
    """Return set of IDs currently in a Postgres table."""
    cur.execute(f"SELECT id FROM {table}")
    return {r[0] for r in cur.fetchall()}


def migrate(table, rows, sq_cols):
    ph = ",".join(["%s"] * len(sq_cols))
    col_list = ",".join(sq_cols)
    cur.executemany(
        f"INSERT INTO {table} ({col_list}) VALUES ({ph}) ON CONFLICT DO NOTHING",
        rows,
    )
    cur.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}','id'),"
        f"COALESCE(MAX(id),1)) FROM {table}"
    )
    pg.commit()
    print(f"{table}: {len(rows)} rows")


# office_details — filter against Postgres source_pages IDs (already migrated)
sp_ids = pg_ids("source_pages")
od_cols = [d[1] for d in sq.execute("PRAGMA table_info(office_details)").fetchall()]
sp_idx = od_cols.index("source_page_id")
od_rows = [r for r in sq.execute("SELECT * FROM office_details").fetchall() if r[sp_idx] in sp_ids]
migrate("office_details", od_rows, od_cols)

# office_table_config — filter against Postgres office_details IDs (just migrated)
od_ids = pg_ids("office_details")
otc_cols = [d[1] for d in sq.execute("PRAGMA table_info(office_table_config)").fetchall()]
od_idx = otc_cols.index("office_details_id")
otc_rows = [
    r for r in sq.execute("SELECT * FROM office_table_config").fetchall() if r[od_idx] in od_ids
]
migrate("office_table_config", otc_rows, otc_cols)

# alt_links — filter against Postgres offices IDs
o_ids = pg_ids("offices")
al_cols = [d[1] for d in sq.execute("PRAGMA table_info(alt_links)").fetchall()]
o_idx = al_cols.index("office_id")
al_rows = [r for r in sq.execute("SELECT * FROM alt_links").fetchall() if r[o_idx] in o_ids]
migrate("alt_links", al_rows, al_cols)

# parser_test_scripts — no FK deps
pts_cols = [d[1] for d in sq.execute("PRAGMA table_info(parser_test_scripts)").fetchall()]
pts_rows = sq.execute("SELECT * FROM parser_test_scripts").fetchall()
migrate("parser_test_scripts", pts_rows, pts_cols)

sq.close()
pg.close()
print("done")
