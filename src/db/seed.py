"""Seed reference data: countries, states, levels, branches."""

from .connection import get_connection


def seed_reference_data(conn=None):
    """Insert default countries, states, levels, branches if empty."""
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cur = conn.execute("SELECT COUNT(*) FROM countries")
        if cur.fetchone()[0] > 0:
            return  # already seeded

        # Levels: federal, state, local
        conn.executemany(
            "INSERT INTO levels (name) VALUES (%s) ON CONFLICT DO NOTHING",
            [("Federal",), ("State",), ("Local",)],
        )
        # Branches: executive, legislative, judicial
        conn.executemany(
            "INSERT INTO branches (name) VALUES (%s) ON CONFLICT DO NOTHING",
            [("Executive",), ("Legislative",), ("Judicial",)],
        )
        # Countries (common for office lists)
        conn.executemany(
            "INSERT INTO countries (name) VALUES (%s) ON CONFLICT DO NOTHING",
            [
                ("United States of America",),
                ("Canada",),
            ],
        )
        conn.commit()

        # States / provinces / territories per country
        cur = conn.execute(
            "SELECT id FROM countries WHERE name = %s", ("United States of America",)
        )
        us_id = cur.fetchone()[0]
        us_states = [
            "Alabama",
            "Alaska",
            "Arizona",
            "Arkansas",
            "California",
            "Colorado",
            "Connecticut",
            "Delaware",
            "Florida",
            "Georgia",
            "Hawaii",
            "Idaho",
            "Illinois",
            "Indiana",
            "Iowa",
            "Kansas",
            "Kentucky",
            "Louisiana",
            "Maine",
            "Maryland",
            "Massachusetts",
            "Michigan",
            "Minnesota",
            "Mississippi",
            "Missouri",
            "Montana",
            "Nebraska",
            "Nevada",
            "New Hampshire",
            "New Jersey",
            "New Mexico",
            "New York",
            "North Carolina",
            "North Dakota",
            "Ohio",
            "Oklahoma",
            "Oregon",
            "Pennsylvania",
            "Rhode Island",
            "South Carolina",
            "South Dakota",
            "Tennessee",
            "Texas",
            "Utah",
            "Vermont",
            "Virginia",
            "Washington",
            "West Virginia",
            "Wisconsin",
            "Wyoming",
            "District of Columbia",
            "Puerto Rico",
            "Guam",
            "American Samoa",
            "U.S. Virgin Islands",
            "Northern Mariana Islands",
        ]
        conn.executemany(
            "INSERT INTO states (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            [(us_id, name) for name in us_states],
        )

        cur = conn.execute("SELECT id FROM countries WHERE name = %s", ("Canada",))
        ca_id = cur.fetchone()[0]
        canada_provinces = [
            "Alberta",
            "British Columbia",
            "Manitoba",
            "New Brunswick",
            "Newfoundland and Labrador",
            "Northwest Territories",
            "Nova Scotia",
            "Nunavut",
            "Ontario",
            "Prince Edward Island",
            "Quebec",
            "Saskatchewan",
            "Yukon",
        ]
        conn.executemany(
            "INSERT INTO states (country_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            [(ca_id, name) for name in canada_provinces],
        )
        conn.commit()
    finally:
        if own_conn:
            conn.close()
