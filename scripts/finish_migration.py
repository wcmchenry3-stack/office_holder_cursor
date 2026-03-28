# -*- coding: utf-8 -*-
"""Finish the partial SQLite→Postgres migration, skipping orphaned FK rows."""

import os
import sqlite3
import psycopg2

sq = sqlite3.connect("/data/office_holder.db")
pg = psycopg2.connect(os.environ["DATABASE_URL"])
cur = pg.cursor()


def migrate(table, sql, info_table):
    cols = [d[1] for d in sq.execute(f"PRAGMA table_info({table})").fetchall()]
    rows = sq.execute(sql).fetchall()
    ph = ",".join(["%s"] * len(cols))
    col_list = ",".join(cols)
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


migrate(
    "office_details",
    """SELECT od.* FROM office_details od
       WHERE EXISTS (SELECT 1 FROM source_pages sp WHERE sp.id = od.source_page_id)""",
    "office_details",
)
migrate(
    "office_table_config",
    """SELECT otc.* FROM office_table_config otc
       WHERE EXISTS (SELECT 1 FROM office_details od WHERE od.id = otc.office_details_id)""",
    "office_table_config",
)
migrate(
    "alt_links",
    """SELECT al.* FROM alt_links al
       WHERE EXISTS (SELECT 1 FROM offices o WHERE o.id = al.office_id)""",
    "alt_links",
)
migrate(
    "parser_test_scripts",
    "SELECT * FROM parser_test_scripts",
    "parser_test_scripts",
)

sq.close()
pg.close()
print("done")
