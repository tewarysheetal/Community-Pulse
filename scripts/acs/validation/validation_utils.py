from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg2
from dotenv import load_dotenv


ACS_YEARS = [2019, 2021, 2022, 2023]
CORE_TABLES = ["b19013", "b25003", "b25070", "dp04"]
BATCH2_TABLES = ["s1101", "s1701", "s1901", "s2301"]
BATCH3_TABLES = ["s1501", "s2401", "b03002", "s0101", "s1601"]
ALL_ACS_TABLES = CORE_TABLES + BATCH2_TABLES + BATCH3_TABLES


GEOID_CANDIDATES = [
    "tract_geoid",
    "geoid",
    "geo_id",
    "geoid10",
    "geoid20",
    "tract_id",
    "census_tract_geoid",
    "tract_fips",
]
YEAR_CANDIDATES = ["year", "survey_year", "acs_year"]
SOURCE_TABLE_CANDIDATES = ["source_table", "acs_table", "table_id", "source"]
METRIC_NAME_CANDIDATES = ["metric_name", "metric_code", "variable_name", "column_name", "metric"]
METRIC_VALUE_CANDIDATES = ["metric_value", "value", "estimate", "metric_estimate"]


@dataclass
class Settings:
    project_root: Path
    output_root: Path
    schema: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    run_ts: str


# ---------------------------------------------------------------------------
# Environment and paths
# ---------------------------------------------------------------------------
def find_project_root(start_path: Path | None = None) -> Path:
    """Try to infer the project root in a flexible way."""
    start = (start_path or Path(__file__)).resolve()
    candidates = [start] + list(start.parents)

    for path in candidates:
        if (path / ".env").exists() and (path / "scripts").exists():
            return path

    for path in candidates:
        if (path / "scripts").exists() or (path / "outputs").exists():
            return path

    # Fallback that matches scripts/acs/validation structure if present.
    if len(start.parents) >= 4:
        return start.parents[3]
    return start.parent



def load_settings() -> Settings:
    project_root = find_project_root(Path(__file__).resolve())
    env_path = project_root / ".env"

    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()

    required = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
    }
    missing = [key for key, value in required.items() if value in (None, "")]
    if missing:
        raise ValueError(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Use load_dotenv() with DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD."
        )

    output_root = project_root / "outputs" / "acs" / "validation"
    output_root.mkdir(parents=True, exist_ok=True)

    return Settings(
        project_root=project_root,
        output_root=output_root,
        schema=os.getenv("DB_SCHEMA", "public"),
        db_host=required["DB_HOST"],
        db_port=int(required["DB_PORT"]),
        db_name=required["DB_NAME"],
        db_user=required["DB_USER"],
        db_password=required["DB_PASSWORD"],
        run_ts=datetime.now().strftime("%Y%m%d_%H%M%S"),
    )



def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path



def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_connection(settings: Settings):
    return psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )



def read_sql(conn, sql: str, params: tuple | list | dict | None = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)



def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'



def qtable(schema: str, table: str) -> str:
    return f"{qident(schema)}.{qident(table)}"



def execute_scalar(conn, sql: str, params: tuple | list | None = None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return row[0] if row else None



def table_exists(conn, schema: str, table: str) -> bool:
    sql = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        )
    """
    return bool(execute_scalar(conn, sql, (schema, table)))



def discover_tables(conn, schema: str, like_pattern: str) -> list[str]:
    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
          AND table_name LIKE %s
        ORDER BY table_name
    """
    df = read_sql(conn, sql, params=(schema, like_pattern))
    return df["table_name"].tolist() if not df.empty else []



def list_columns(conn, schema: str, table: str) -> pd.DataFrame:
    sql = """
        SELECT
            column_name,
            data_type,
            udt_name,
            is_nullable,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
    """
    return read_sql(conn, sql, params=(schema, table))


# ---------------------------------------------------------------------------
# Table / column detection
# ---------------------------------------------------------------------------
def pick_column(available: Iterable[str], candidates: Iterable[str]) -> str | None:
    available_map = {col.lower(): col for col in available}
    for candidate in candidates:
        if candidate.lower() in available_map:
            return available_map[candidate.lower()]
    return None



def parse_year_and_source(table_name: str) -> tuple[int | None, str | None]:
    patterns = [
        r"^stg_acs_(\d{4})_([a-z0-9]+)_raw$",
        r"^int_acs_(\d{4})_([a-z0-9]+)$",
        r"^vw_int_acs_([a-z0-9]+)_all_years$",
    ]
    for pattern in patterns:
        match = re.match(pattern, table_name)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                year = int(groups[0]) if groups[0].isdigit() else None
                source_table = groups[1]
                return year, source_table
            if len(groups) == 1:
                return None, groups[0]
    return None, None



def batch_group_for_source(source_table: str | None) -> str | None:
    if source_table in CORE_TABLES:
        return "core"
    if source_table in BATCH2_TABLES:
        return "batch_2"
    if source_table in BATCH3_TABLES:
        return "batch_3"
    return None



def detect_table_columns(conn, schema: str, table: str) -> dict[str, str | None]:
    cols = list_columns(conn, schema, table)
    names = cols["column_name"].tolist()
    return {
        "geoid_col": pick_column(names, GEOID_CANDIDATES),
        "year_col": pick_column(names, YEAR_CANDIDATES),
        "source_table_col": pick_column(names, SOURCE_TABLE_CANDIDATES),
        "metric_name_col": pick_column(names, METRIC_NAME_CANDIDATES),
        "metric_value_col": pick_column(names, METRIC_VALUE_CANDIDATES),
    }



def get_numeric_columns(conn, schema: str, table: str) -> list[str]:
    cols = list_columns(conn, schema, table)
    numeric_types = {
        "smallint",
        "integer",
        "bigint",
        "numeric",
        "decimal",
        "real",
        "double precision",
    }
    return cols.loc[cols["data_type"].isin(numeric_types), "column_name"].tolist()



def get_text_like_columns(conn, schema: str, table: str) -> list[str]:
    cols = list_columns(conn, schema, table)
    text_types = {"text", "character varying", "character"}
    return cols.loc[cols["data_type"].isin(text_types), "column_name"].tolist()



def bridge_columns(conn, schema: str, bridge_table: str = "bridge_tract_year") -> tuple[str | None, str | None]:
    if not table_exists(conn, schema, bridge_table):
        return None, None
    cols = list_columns(conn, schema, bridge_table)
    names = cols["column_name"].tolist()
    geoid_col = pick_column(names, GEOID_CANDIDATES)
    year_col = pick_column(names, YEAR_CANDIDATES)
    return geoid_col, year_col


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------
def nonmissing_text_expr(column: str) -> str:
    col = qident(column)
    return f"NULLIF(BTRIM({col}::text), '') IS NOT NULL"



def missing_text_expr(column: str) -> str:
    col = qident(column)
    return f"NULLIF(BTRIM({col}::text), '') IS NULL"



def row_count(conn, schema: str, table: str) -> int:
    sql = f"SELECT COUNT(*) FROM {qtable(schema, table)}"
    return int(execute_scalar(conn, sql) or 0)



def distinct_nonmissing_count(conn, schema: str, table: str, column: str) -> int:
    sql = f"""
        SELECT COUNT(DISTINCT {qident(column)}::text)
        FROM {qtable(schema, table)}
        WHERE {nonmissing_text_expr(column)}
    """
    return int(execute_scalar(conn, sql) or 0)



def null_count(conn, schema: str, table: str, column: str) -> int:
    sql = f"SELECT COUNT(*) FROM {qtable(schema, table)} WHERE {qident(column)} IS NULL"
    return int(execute_scalar(conn, sql) or 0)



def blank_count(conn, schema: str, table: str, column: str) -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {qtable(schema, table)}
        WHERE {qident(column)} IS NOT NULL
          AND NULLIF(BTRIM({qident(column)}::text), '') IS NULL
    """
    return int(execute_scalar(conn, sql) or 0)



def duplicate_stats(conn, schema: str, table: str, key_cols: list[str]) -> tuple[int, int]:
    if not key_cols:
        return 0, 0
    group_cols = ", ".join(f"{qident(col)}::text" for col in key_cols)
    where_clause = " AND ".join(nonmissing_text_expr(col) for col in key_cols)
    sql = f"""
        WITH d AS (
            SELECT {group_cols}, COUNT(*) AS cnt
            FROM {qtable(schema, table)}
            WHERE {where_clause}
            GROUP BY {group_cols}
            HAVING COUNT(*) > 1
        )
        SELECT COALESCE(COUNT(*), 0) AS duplicate_groups,
               COALESCE(SUM(cnt - 1), 0) AS duplicate_excess_rows
        FROM d
    """
    df = read_sql(conn, sql)
    if df.empty:
        return 0, 0
    return int(df.loc[0, "duplicate_groups"]), int(df.loc[0, "duplicate_excess_rows"])



def bridge_coverage(
    conn,
    schema: str,
    table: str,
    source_geoid_col: str | None,
    source_year: int | None,
    bridge_table: str = "bridge_tract_year",
) -> dict[str, int | None]:
    bridge_geoid_col, bridge_year_col = bridge_columns(conn, schema, bridge_table)
    if not source_geoid_col or source_year is None or not bridge_geoid_col or not bridge_year_col:
        return {
            "src_distinct_geoid_count": None,
            "bridge_distinct_geoid_count": None,
            "matched_geoid_count": None,
            "missing_in_bridge_geoid_count": None,
            "missing_in_source_geoid_count": None,
        }

    sql = f"""
        WITH src AS (
            SELECT DISTINCT {qident(source_geoid_col)}::text AS tract_geoid
            FROM {qtable(schema, table)}
            WHERE {nonmissing_text_expr(source_geoid_col)}
        ),
        br AS (
            SELECT DISTINCT {qident(bridge_geoid_col)}::text AS tract_geoid
            FROM {qtable(schema, bridge_table)}
            WHERE {qident(bridge_year_col)} = %s
              AND {nonmissing_text_expr(bridge_geoid_col)}
        )
        SELECT
            (SELECT COUNT(*) FROM src) AS src_distinct_geoid_count,
            (SELECT COUNT(*) FROM br) AS bridge_distinct_geoid_count,
            (SELECT COUNT(*) FROM src s INNER JOIN br b ON s.tract_geoid = b.tract_geoid) AS matched_geoid_count,
            (SELECT COUNT(*) FROM src s LEFT JOIN br b ON s.tract_geoid = b.tract_geoid WHERE b.tract_geoid IS NULL) AS missing_in_bridge_geoid_count,
            (SELECT COUNT(*) FROM br b LEFT JOIN src s ON s.tract_geoid = b.tract_geoid WHERE s.tract_geoid IS NULL) AS missing_in_source_geoid_count
    """
    df = read_sql(conn, sql, params=(source_year,))
    if df.empty:
        return {
            "src_distinct_geoid_count": None,
            "bridge_distinct_geoid_count": None,
            "matched_geoid_count": None,
            "missing_in_bridge_geoid_count": None,
            "missing_in_source_geoid_count": None,
        }
    return df.iloc[0].to_dict()



def missing_profile_df(conn, schema: str, table: str, columns: list[str]) -> pd.DataFrame:
    if not columns:
        return pd.DataFrame(columns=["column_name", "null_count", "missing_count", "row_count"])

    union_parts = []
    for col in columns:
        union_parts.append(
            f"""
            SELECT
                '{col}' AS column_name,
                COUNT(*) FILTER (WHERE {qident(col)} IS NULL) AS null_count,
                COUNT(*) FILTER (WHERE {missing_text_expr(col)}) AS missing_count,
                COUNT(*) AS row_count
            FROM {qtable(schema, table)}
            """.strip()
        )
    sql = " UNION ALL ".join(union_parts)
    return read_sql(conn, sql)



def numeric_profile_df(conn, schema: str, table: str, numeric_columns: list[str]) -> pd.DataFrame:
    if not numeric_columns:
        return pd.DataFrame(
            columns=["column_name", "null_count", "zero_count", "negative_count", "min_value", "max_value"]
        )

    union_parts = []
    for col in numeric_columns:
        union_parts.append(
            f"""
            SELECT
                '{col}' AS column_name,
                COUNT(*) FILTER (WHERE {qident(col)} IS NULL) AS null_count,
                COUNT(*) FILTER (WHERE {qident(col)} = 0) AS zero_count,
                COUNT(*) FILTER (WHERE {qident(col)} < 0) AS negative_count,
                MIN({qident(col)})::text AS min_value,
                MAX({qident(col)})::text AS max_value
            FROM {qtable(schema, table)}
            """.strip()
        )
    sql = " UNION ALL ".join(union_parts)
    return read_sql(conn, sql)



def year_counts_df(conn, schema: str, table: str, year_col: str) -> pd.DataFrame:
    sql = f"""
        SELECT {qident(year_col)} AS year, COUNT(*) AS row_count
        FROM {qtable(schema, table)}
        GROUP BY {qident(year_col)}
        ORDER BY {qident(year_col)}
    """
    return read_sql(conn, sql)



def source_year_counts_df(conn, schema: str, table: str, year_col: str, source_col: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            {qident(year_col)} AS year,
            LOWER({qident(source_col)}::text) AS source_table,
            COUNT(*) AS row_count
        FROM {qtable(schema, table)}
        GROUP BY {qident(year_col)}, LOWER({qident(source_col)}::text)
        ORDER BY {qident(year_col)}, LOWER({qident(source_col)}::text)
    """
    return read_sql(conn, sql)



def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log(f"Saved: {path}")



def write_run_summary(records: list[dict], path: Path) -> None:
    df = pd.DataFrame(records)
    write_csv(df, path)



def expected_table_manifest(prefix: str) -> pd.DataFrame:
    rows = []
    for year in ACS_YEARS:
        for source_table in ALL_ACS_TABLES:
            if prefix == "stg":
                table_name = f"stg_acs_{year}_{source_table}_raw"
            elif prefix == "int":
                table_name = f"int_acs_{year}_{source_table}"
            else:
                raise ValueError("prefix must be either 'stg' or 'int'")
            rows.append(
                {
                    "table_name": table_name,
                    "year": year,
                    "source_table": source_table,
                    "batch_group": batch_group_for_source(source_table),
                }
            )
    return pd.DataFrame(rows)



def add_exists_flag(expected_df: pd.DataFrame, discovered_tables: list[str]) -> pd.DataFrame:
    discovered_set = set(discovered_tables)
    out = expected_df.copy()
    out["exists"] = out["table_name"].isin(discovered_set)
    return out



def cli_error(exc: Exception) -> None:
    print(f"\nERROR: {exc}\n", file=sys.stderr)
    raise SystemExit(1)
