from __future__ import annotations

import argparse
import os
from io import StringIO
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

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

STANDARD_ID_COLUMNS = [
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

METRIC_GROUP_MAP = {
    "b19013": "income",
    "b25003": "tenure",
    "b25070": "rent_burden",
    "dp04": "housing_profile",
    "s1101": "household_composition",
    "s1701": "poverty",
    "s1901": "income_distribution",
    "s2301": "employment",
    "s1501": "education",
    "s2401": "occupation",
    "b03002": "race_ethnicity",
    "s0101": "age",
    "s1601": "language",
}


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


def get_target_schema() -> str:
    return os.getenv("DB_SCHEMA", "public")


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def connect_db(db_config: dict):
    return psycopg2.connect(**db_config)


def relation_exists(conn, relation_name: str, schema: str) -> bool:
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


def get_relation_columns(conn, relation_name: str, schema: str) -> list[str]:
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, relation_name))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_view_df(conn, table_code: str, schema: str) -> pd.DataFrame:
    view_name = f"vw_int_acs_{table_code}_all_years"
    if not relation_exists(conn, view_name, schema):
        raise ValueError(f"Required view missing: {schema}.{view_name}")

    order_cols = []
    available_cols = get_relation_columns(conn, view_name, schema)
    for col in ["year", "tractce", "tract_geoid"]:
        if col in available_cols:
            order_cols.append(col)

    order_sql = f" ORDER BY {', '.join(order_cols)}" if order_cols else ""
    query = f'SELECT * FROM {schema}."{view_name}"{order_sql}'
    return read_sql(conn, query)


def infer_metric_kind(metric_name: str) -> str:
    metric_name = str(metric_name).lower()
    if metric_name.endswith("_m"):
        return "moe"
    if metric_name.endswith("_e"):
        return "estimate"
    return "derived"


def melt_int_view_to_long(df: pd.DataFrame, table_code: str) -> pd.DataFrame:
    available_id_cols = [c for c in STANDARD_ID_COLUMNS if c in df.columns]
    if "year" not in available_id_cols or "tract_geoid" not in available_id_cols:
        raise ValueError(
            f"vw_int_acs_{table_code}_all_years must include at least 'year' and 'tract_geoid'"
        )

    metric_cols = [c for c in df.columns if c not in available_id_cols]
    if not metric_cols:
        raise ValueError(f"No metric columns found for {table_code}")

    long_df = df.melt(
        id_vars=available_id_cols,
        value_vars=metric_cols,
        var_name="metric_name",
        value_name="metric_value",
    )

    long_df["metric_value"] = pd.to_numeric(long_df["metric_value"], errors="coerce")
    long_df = long_df.dropna(subset=["metric_value"]).copy()

    long_df["source_table"] = table_code.upper()
    long_df["metric_group"] = METRIC_GROUP_MAP.get(table_code, "other")
    long_df["metric_kind"] = long_df["metric_name"].apply(infer_metric_kind)

    # Standardize final columns
    final_cols = [
        "year",
        "tract_geoid",
        "source_table",
        "metric_group",
        "metric_name",
        "metric_kind",
        "metric_value",
    ]

    for optional_col in ["geo_id", "name", "statefp", "countyfp", "tractce"]:
        if optional_col in long_df.columns:
            final_cols.append(optional_col)

    long_df = long_df[final_cols].sort_values(
        ["year", "tract_geoid", "source_table", "metric_name"]
    ).reset_index(drop=True)

    return long_df


def build_all_long_df(conn, schema: str) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []

    for table_code in ALL_TABLE_CODES:
        df = get_view_df(conn, table_code, schema)
        long_df = melt_int_view_to_long(df, table_code)
        parts.append(long_df)

        print(
            f"[LONG] {table_code.upper()}: "
            f"input_rows={len(df)}, output_rows={len(long_df)}"
        )

    final_df = pd.concat(parts, ignore_index=True)
    final_df = final_df.sort_values(
        ["year", "tract_geoid", "source_table", "metric_name"]
    ).reset_index(drop=True)
    return final_df


def build_create_table_sql(schema: str) -> str:
    return f"""
DROP VIEW IF EXISTS {schema}.vw_acs_tract_metric_long_stable_4yr;
DROP VIEW IF EXISTS {schema}.vw_acs_tract_metric_long_2023;
DROP VIEW IF EXISTS {schema}.vw_acs_tract_metric_long_all_years;

DROP TABLE IF EXISTS {schema}.fact_acs_tract_metric_long;

CREATE TABLE {schema}.fact_acs_tract_metric_long (
    year            INTEGER NOT NULL,
    tract_geoid     VARCHAR(11) NOT NULL,
    source_table    VARCHAR(20) NOT NULL,
    metric_group    VARCHAR(50) NOT NULL,
    metric_name     VARCHAR(255) NOT NULL,
    metric_kind     VARCHAR(20) NOT NULL,
    metric_value    NUMERIC,
    geo_id          VARCHAR(20),
    name            TEXT,
    statefp         VARCHAR(2),
    countyfp        VARCHAR(3),
    tractce         VARCHAR(6),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_acs_tract_metric_long_key
    ON {schema}.fact_acs_tract_metric_long (year, tract_geoid);

CREATE INDEX idx_fact_acs_tract_metric_long_metric
    ON {schema}.fact_acs_tract_metric_long (source_table, metric_name);

CREATE INDEX idx_fact_acs_tract_metric_long_group
    ON {schema}.fact_acs_tract_metric_long (metric_group, metric_kind);

CREATE VIEW {schema}.vw_acs_tract_metric_long_all_years AS
SELECT
    f.*,
    d.tract_number,
    d.tract_name_canonical,
    d.tract_name_latest,
    d.county_name,
    d.state_name,
    d.year_count,
    d.is_stable_all_4_years,
    y.year_label,
    y.acs_dataset,
    y.acs_period
FROM {schema}.fact_acs_tract_metric_long f
LEFT JOIN {schema}.dim_tract d
  ON d.tract_geoid = f.tract_geoid
LEFT JOIN {schema}.dim_year y
  ON y.year = f.year;

CREATE VIEW {schema}.vw_acs_tract_metric_long_2023 AS
SELECT *
FROM {schema}.vw_acs_tract_metric_long_all_years
WHERE year = 2023;

CREATE VIEW {schema}.vw_acs_tract_metric_long_stable_4yr AS
SELECT *
FROM {schema}.vw_acs_tract_metric_long_all_years
WHERE is_stable_all_4_years = 1;
""".strip()


def execute_sql(conn, sql_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql_text)
    conn.commit()


def read_sql(conn, query: str, params=None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def load_long_df_to_postgres(conn, df: pd.DataFrame, schema: str) -> None:
    expected_cols = [
        "year",
        "tract_geoid",
        "source_table",
        "metric_group",
        "metric_name",
        "metric_kind",
        "metric_value",
        "geo_id",
        "name",
        "statefp",
        "countyfp",
        "tractce",
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    load_df = df[expected_cols].copy()

    buffer = StringIO()
    load_df.to_csv(buffer, index=False, header=True)
    buffer.seek(0)

    copy_sql = f"""
    COPY {schema}.fact_acs_tract_metric_long (
        year,
        tract_geoid,
        source_table,
        metric_group,
        metric_name,
        metric_kind,
        metric_value,
        geo_id,
        name,
        statefp,
        countyfp,
        tractce
    )
    FROM STDIN WITH CSV HEADER DELIMITER ','
    """

    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buffer)
    conn.commit()


def export_validation_outputs(
    conn,
    project_root: Path,
    long_df: pd.DataFrame,
    schema: str,
) -> None:
    out_dir = project_root / "outputs" / "acs" / "acs_tract"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_df = read_sql(
        conn,
        f"""
        SELECT
            year,
            source_table,
            metric_group,
            COUNT(*) AS row_count,
            COUNT(DISTINCT metric_name) AS distinct_metric_count
        FROM {schema}.fact_acs_tract_metric_long
        GROUP BY year, source_table, metric_group
        ORDER BY year, source_table
        """,
    )
    summary_df.to_csv(out_dir / "fact_acs_tract_metric_long_summary.csv", index=False)

    overall_df = read_sql(
        conn,
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT year) AS distinct_years,
            COUNT(DISTINCT tract_geoid) AS distinct_tracts,
            COUNT(DISTINCT source_table) AS distinct_source_tables,
            COUNT(DISTINCT metric_name) AS distinct_metrics
        FROM {schema}.fact_acs_tract_metric_long
        """,
    )
    overall_df.to_csv(out_dir / "fact_acs_tract_metric_long_overall.csv", index=False)

    sample_df = read_sql(
        conn,
        f"""
        SELECT *
        FROM {schema}.vw_acs_tract_metric_long_all_years
        ORDER BY year, tractce, source_table, metric_name
        LIMIT 100
        """,
    )
    sample_df.to_csv(out_dir / "fact_acs_tract_metric_long_sample.csv", index=False)

    metric_dict_df = (
        long_df[["source_table", "metric_group", "metric_name", "metric_kind"]]
        .drop_duplicates()
        .sort_values(["source_table", "metric_name"])
        .reset_index(drop=True)
    )
    metric_dict_df.to_csv(out_dir / "fact_acs_tract_metric_long_metric_dictionary.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-sql", default="true")
    args = parser.parse_args()

    run_sql = parse_bool(args.run_sql)

    project_root = find_project_root()
    load_env_file(project_root)

    db_config = get_db_config()
    validate_db_config(db_config)
    target_schema = get_target_schema()

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
            if not relation_exists(conn, view_name, target_schema):
                raise ValueError(f"Required view missing: {target_schema}.{view_name}")

        long_df = build_all_long_df(conn, target_schema)
        print(f"[OK] Final long dataframe rows: {len(long_df)}")

        sql_text = build_create_table_sql(target_schema)
        sql_file = sql_dir / "06_create_fact_acs_tract_metric_long.sql"
        sql_file.write_text(sql_text + "\n", encoding="utf-8")
        print(f"[OK] SQL written to: {sql_file}")

        parquet_like_csv = out_dir / "fact_acs_tract_metric_long_preview.csv"
        long_df.head(1000).to_csv(parquet_like_csv, index=False)

        if run_sql:
            execute_sql(conn, sql_text)
            print("[RUN] fact_acs_tract_metric_long table and views created")

            load_long_df_to_postgres(conn, long_df, target_schema)
            print("[RUN] Long fact data loaded")

            export_validation_outputs(conn, project_root, long_df, target_schema)
            print("[OK] Validation outputs written")

            validation_df = read_sql(
                conn,
                f"""
                SELECT
                    year,
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT metric_name) AS distinct_metric_count
                FROM {target_schema}.fact_acs_tract_metric_long
                GROUP BY year
                ORDER BY year
                """,
            )
            print(validation_df.to_string(index=False))

    finally:
        conn.close()

    print("\nDone.")
    print(f"[OK] Preview CSV: {out_dir / 'fact_acs_tract_metric_long_preview.csv'}")
    print(f"[OK] Summary CSV: {out_dir / 'fact_acs_tract_metric_long_summary.csv'}")
    print(f"[OK] Overall CSV: {out_dir / 'fact_acs_tract_metric_long_overall.csv'}")
    print(f"[OK] Sample CSV: {out_dir / 'fact_acs_tract_metric_long_sample.csv'}")
    print(f"[OK] Metric dictionary CSV: {out_dir / 'fact_acs_tract_metric_long_metric_dictionary.csv'}")


if __name__ == "__main__":
    main()