from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

TARGET_SCHEMA = "public"

ALL_TABLE_CODES = [
    "b19013",
    "b25003",
    "b25070",
    "dp04",
    "s1101",
    "s1701",
    "s1901",
    "s2301",
    "s1501",
    "s2401",
    "b03002",
    "s0101",
    "s1601",
]

JSON_SKIP_KEYS = [
    "year",
    "tract_geoid",
    "geo_id",
    "name",
    "statefp",
    "countyfp",
    "tractce",
    "table_code",
    "table_family",
    "source_type",
]


def find_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def load_env_file(project_root: Path) -> None:
    env_file = project_root / ".env"
    if not env_file.exists():
        raise FileNotFoundError(f".env file not found at: {env_file}")
    load_dotenv(env_file)
    print(f"[ENV] Loaded .env from: {env_file}")


def get_db_config() -> dict:
    return {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }


def validate_db_config(db_config: dict) -> None:
    missing = [k for k, v in db_config.items() if v in {None, ""}]
    if missing:
        raise ValueError(f"Missing DB config values in .env: {missing}")


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def connect_db(db_config: dict):
    return psycopg2.connect(**db_config)


def relation_exists(conn, relation_name: str, schema: str = TARGET_SCHEMA) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.views
        WHERE table_schema = %s
          AND table_name = %s
        UNION ALL
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, relation_name, schema, relation_name))
        return cur.fetchone() is not None


def jsonb_payload_expr(alias: str, output_name: str) -> str:
    keys_sql = ", ".join(f"'{k}'" for k in JSON_SKIP_KEYS)
    return (
        f"    CASE "
        f"WHEN {alias}.tract_geoid IS NOT NULL "
        f"THEN to_jsonb({alias}) - ARRAY[{keys_sql}] "
        f"ELSE NULL "
        f"END AS {output_name}"
    )


def build_fact_sql() -> str:
    jsonb_lines = [
        jsonb_payload_expr("b19013", "b19013_metrics_json"),
        jsonb_payload_expr("b25003", "b25003_metrics_json"),
        jsonb_payload_expr("b25070", "b25070_metrics_json"),
        jsonb_payload_expr("dp04", "dp04_metrics_json"),
        jsonb_payload_expr("s1101", "s1101_metrics_json"),
        jsonb_payload_expr("s1701", "s1701_metrics_json"),
        jsonb_payload_expr("s1901", "s1901_metrics_json"),
        jsonb_payload_expr("s2301", "s2301_metrics_json"),
        jsonb_payload_expr("s1501", "s1501_metrics_json"),
        jsonb_payload_expr("s2401", "s2401_metrics_json"),
        jsonb_payload_expr("b03002", "b03002_metrics_json"),
        jsonb_payload_expr("s0101", "s0101_metrics_json"),
        jsonb_payload_expr("s1601", "s1601_metrics_json"),
    ]
    jsonb_sql = ",\n".join(jsonb_lines)

    return f"""
DROP VIEW IF EXISTS {TARGET_SCHEMA}.vw_acs_tract_profile_cluster_input;
DROP VIEW IF EXISTS {TARGET_SCHEMA}.vw_acs_tract_profile_stable_4yr;
DROP VIEW IF EXISTS {TARGET_SCHEMA}.vw_acs_tract_profile_2023;
DROP VIEW IF EXISTS {TARGET_SCHEMA}.vw_acs_tract_profile_all_years;

DROP TABLE IF EXISTS {TARGET_SCHEMA}.fact_acs_tract_profile;

CREATE TABLE {TARGET_SCHEMA}.fact_acs_tract_profile AS
WITH base AS (
    SELECT
        b.year,
        b.tract_geoid,
        d.geo_id,
        d.statefp,
        d.countyfp,
        d.tractce,
        d.tract_number,
        d.tract_name_canonical,
        d.tract_name_latest,
        d.county_name,
        d.state_name,
        d.first_year_seen,
        d.last_year_seen,
        d.year_count,
        d.is_stable_all_4_years,
        y.year_label,
        y.acs_dataset,
        y.acs_period
    FROM {TARGET_SCHEMA}.bridge_tract_year b
    JOIN {TARGET_SCHEMA}.dim_tract d
      ON d.tract_geoid = b.tract_geoid
    JOIN {TARGET_SCHEMA}.dim_year y
      ON y.year = b.year
)
SELECT
    base.year,
    base.tract_geoid,
    base.geo_id,
    base.statefp,
    base.countyfp,
    base.tractce,
    base.tract_number,
    base.tract_name_canonical,
    base.tract_name_latest,
    base.county_name,
    base.state_name,
    base.first_year_seen,
    base.last_year_seen,
    base.year_count,
    base.is_stable_all_4_years,
    base.year_label,
    base.acs_dataset,
    base.acs_period,

    -- Curated wide metrics for EDA / clustering
    b19013.median_household_income,
    b19013.median_household_income_moe,

    b25003.occupied_units,
    b25003.owner_occupied_units,
    b25003.renter_occupied_units,
    b25003.pct_owner_occupied,
    b25003.pct_renter_occupied,

    b25070.renter_hh_rent_burden_base,
    b25070.rent_30_34,
    b25070.rent_35_39,
    b25070.rent_40_49,
    b25070.rent_50_plus,
    b25070.rent_not_computed,
    b25070.pct_rent_burden_30_plus,
    b25070.pct_rent_burden_50_plus,
    b25070.pct_rent_not_computed,

    dp04.housing_units_total,
    dp04.occupied_housing_units_dp04,
    dp04.vacant_housing_units_dp04,
    dp04.pct_occupied_housing_units,
    dp04.pct_vacant_housing_units,
    dp04.for_rent_units,
    dp04.rented_not_occupied_units,
    dp04.for_sale_only_units,
    dp04.sold_not_occupied_units,
    dp04.seasonal_recreational_units,
    dp04.migrant_worker_units,
    dp04.other_vacant_units,

    -- Reliable demographic metrics from B03002
    b03002.b03002_001e AS total_population,
    b03002.b03002_002e AS not_hispanic_total,
    b03002.b03002_003e AS white_non_hispanic_population,
    b03002.b03002_004e AS black_non_hispanic_population,
    b03002.b03002_012e AS hispanic_population,

    CASE
        WHEN b03002.b03002_001e > 0
        THEN ROUND(100.0 * b03002.b03002_003e / b03002.b03002_001e, 2)
        ELSE NULL
    END AS pct_white_non_hispanic,

    CASE
        WHEN b03002.b03002_001e > 0
        THEN ROUND(100.0 * b03002.b03002_004e / b03002.b03002_001e, 2)
        ELSE NULL
    END AS pct_black_non_hispanic,

    CASE
        WHEN b03002.b03002_001e > 0
        THEN ROUND(100.0 * b03002.b03002_012e / b03002.b03002_001e, 2)
        ELSE NULL
    END AS pct_hispanic,

    CASE
        WHEN b19013.median_household_income IS NOT NULL
         AND b25003.occupied_units IS NOT NULL
         AND b25070.renter_hh_rent_burden_base IS NOT NULL
         AND dp04.housing_units_total IS NOT NULL
        THEN 1 ELSE 0
    END AS has_core_housing_metrics,

    CASE
        WHEN s1101.tract_geoid IS NOT NULL
         AND s1701.tract_geoid IS NOT NULL
         AND s1901.tract_geoid IS NOT NULL
         AND s2301.tract_geoid IS NOT NULL
        THEN 1 ELSE 0
    END AS has_batch2_metrics,

    CASE
        WHEN s1501.tract_geoid IS NOT NULL
         AND s2401.tract_geoid IS NOT NULL
         AND b03002.tract_geoid IS NOT NULL
         AND s0101.tract_geoid IS NOT NULL
         AND s1601.tract_geoid IS NOT NULL
        THEN 1 ELSE 0
    END AS has_batch3_metrics,

{jsonb_sql}

FROM base
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_b19013_all_years b19013
  ON b19013.year = base.year
 AND b19013.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_b25003_all_years b25003
  ON b25003.year = base.year
 AND b25003.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_b25070_all_years b25070
  ON b25070.year = base.year
 AND b25070.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_dp04_all_years dp04
  ON dp04.year = base.year
 AND dp04.tract_geoid = base.tract_geoid

LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s1101_all_years s1101
  ON s1101.year = base.year
 AND s1101.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s1701_all_years s1701
  ON s1701.year = base.year
 AND s1701.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s1901_all_years s1901
  ON s1901.year = base.year
 AND s1901.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s2301_all_years s2301
  ON s2301.year = base.year
 AND s2301.tract_geoid = base.tract_geoid

LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s1501_all_years s1501
  ON s1501.year = base.year
 AND s1501.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s2401_all_years s2401
  ON s2401.year = base.year
 AND s2401.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_b03002_all_years b03002
  ON b03002.year = base.year
 AND b03002.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s0101_all_years s0101
  ON s0101.year = base.year
 AND s0101.tract_geoid = base.tract_geoid
LEFT JOIN {TARGET_SCHEMA}.vw_int_acs_s1601_all_years s1601
  ON s1601.year = base.year
 AND s1601.tract_geoid = base.tract_geoid
;

CREATE INDEX idx_fact_acs_tract_profile_key
    ON {TARGET_SCHEMA}.fact_acs_tract_profile (year, tract_geoid);

CREATE INDEX idx_fact_acs_tract_profile_year
    ON {TARGET_SCHEMA}.fact_acs_tract_profile (year);

CREATE INDEX idx_fact_acs_tract_profile_stable
    ON {TARGET_SCHEMA}.fact_acs_tract_profile (is_stable_all_4_years, year);

CREATE VIEW {TARGET_SCHEMA}.vw_acs_tract_profile_all_years AS
SELECT *
FROM {TARGET_SCHEMA}.fact_acs_tract_profile;

CREATE VIEW {TARGET_SCHEMA}.vw_acs_tract_profile_2023 AS
SELECT *
FROM {TARGET_SCHEMA}.fact_acs_tract_profile
WHERE year = 2023;

CREATE VIEW {TARGET_SCHEMA}.vw_acs_tract_profile_stable_4yr AS
SELECT *
FROM {TARGET_SCHEMA}.fact_acs_tract_profile
WHERE is_stable_all_4_years = 1;

CREATE VIEW {TARGET_SCHEMA}.vw_acs_tract_profile_cluster_input AS
SELECT
    year,
    tract_geoid,
    tractce,
    tract_number,
    tract_name_canonical,
    is_stable_all_4_years,
    year_count,
    median_household_income,
    pct_owner_occupied,
    pct_renter_occupied,
    pct_rent_burden_30_plus,
    pct_rent_burden_50_plus,
    pct_vacant_housing_units,
    pct_white_non_hispanic,
    pct_black_non_hispanic,
    pct_hispanic,
    has_core_housing_metrics,
    has_batch2_metrics,
    has_batch3_metrics
FROM {TARGET_SCHEMA}.fact_acs_tract_profile;
""".strip()


def execute_sql(conn, sql_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def read_sql(conn, query: str, params=None):
    import pandas as pd
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def export_validation_outputs(conn, project_root: Path) -> None:
    out_dir = project_root / "outputs" / "acs" / "acs_tract"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_df = read_sql(
        conn,
        f"""
        SELECT
            year,
            COUNT(*) AS row_count,
            SUM(has_core_housing_metrics) AS rows_with_core_housing_metrics,
            SUM(has_batch2_metrics) AS rows_with_batch2_metrics,
            SUM(has_batch3_metrics) AS rows_with_batch3_metrics,
            ROUND(AVG(median_household_income), 2) AS avg_median_household_income,
            ROUND(AVG(pct_renter_occupied), 2) AS avg_pct_renter_occupied,
            ROUND(AVG(pct_rent_burden_30_plus), 2) AS avg_pct_rent_burden_30_plus,
            ROUND(AVG(pct_hispanic), 2) AS avg_pct_hispanic
        FROM {TARGET_SCHEMA}.fact_acs_tract_profile
        GROUP BY year
        ORDER BY year
        """,
    )
    summary_df.to_csv(out_dir / "fact_acs_tract_profile_validation_summary.csv", index=False)

    col_df = read_sql(
        conn,
        f"""
        SELECT
            ordinal_position,
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = '{TARGET_SCHEMA}'
          AND table_name = 'fact_acs_tract_profile'
        ORDER BY ordinal_position
        """,
    )
    col_df.to_csv(out_dir / "fact_acs_tract_profile_column_dictionary.csv", index=False)

    sample_df = read_sql(
        conn,
        f"""
        SELECT
            year,
            tract_geoid,
            tract_name_canonical,
            median_household_income,
            pct_renter_occupied,
            pct_rent_burden_30_plus,
            pct_hispanic
        FROM {TARGET_SCHEMA}.fact_acs_tract_profile
        ORDER BY year, tractce
        LIMIT 25
        """,
    )
    sample_df.to_csv(out_dir / "fact_acs_tract_profile_sample.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-sql", default="true")
    args = parser.parse_args()

    run_sql = parse_bool(args.run_sql)

    project_root = find_project_root()
    load_env_file(project_root)

    db_config = get_db_config()
    validate_db_config(db_config)

    sql_dir = project_root / "sql" / "acs" / "fact"
    out_dir = project_root / "outputs" / "acs" / "acs_tract"
    sql_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = connect_db(db_config)
    print(
        f"[DB] Connected to {db_config['dbname']} at "
        f"{db_config['host']}:{db_config['port']} as {db_config['user']}"
    )

    try:
        for table_code in ALL_TABLE_CODES:
            view_name = f"vw_int_acs_{table_code}_all_years"
            if not relation_exists(conn, view_name, TARGET_SCHEMA):
                raise ValueError(f"Required view missing: {TARGET_SCHEMA}.{view_name}")

        sql_text = build_fact_sql()
        sql_file = sql_dir / "07_create_fact_acs_tract_profile.sql"
        sql_file.write_text(sql_text + "\n", encoding="utf-8")
        print(f"[OK] SQL written to: {sql_file}")

        if run_sql:
            execute_sql(conn, sql_text)
            print("[RUN] fact_acs_tract_profile and views created")
            export_validation_outputs(conn, project_root)
            print("[OK] Validation outputs written")

            validation_df = read_sql(
                conn,
                f"""
                SELECT year, COUNT(*) AS row_count
                FROM {TARGET_SCHEMA}.fact_acs_tract_profile
                GROUP BY year
                ORDER BY year
                """,
            )
            print(validation_df.to_string(index=False))

    finally:
        conn.close()

    print("\nDone.")
    print(f"[OK] Validation summary: {out_dir / 'fact_acs_tract_profile_validation_summary.csv'}")
    print(f"[OK] Column dictionary: {out_dir / 'fact_acs_tract_profile_column_dictionary.csv'}")
    print(f"[OK] Sample extract: {out_dir / 'fact_acs_tract_profile_sample.csv'}")


if __name__ == "__main__":
    main()