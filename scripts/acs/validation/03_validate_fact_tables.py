from __future__ import annotations

import pandas as pd

from validation_utils import (
    ACS_YEARS,
    ALL_ACS_TABLES,
    batch_group_for_source,
    blank_count,
    bridge_columns,
    cli_error,
    detect_table_columns,
    distinct_nonmissing_count,
    duplicate_stats,
    ensure_dir,
    get_connection,
    get_numeric_columns,
    load_settings,
    log,
    missing_profile_df,
    null_count,
    numeric_profile_df,
    qident,
    qtable,
    read_sql,
    row_count,
    source_year_counts_df,
    table_exists,
    write_csv,
    write_run_summary,
    year_counts_df,
)


OUTPUT_SUBDIR = "fact"
LONG_FACT = "fact_acs_tract_metric_long"
PROFILE_FACT = "fact_acs_tract_profile"
BRIDGE_TABLE = "bridge_tract_year"


def _make_coverage_flags(df: pd.DataFrame) -> pd.DataFrame:
    expected = pd.DataFrame([(y, s) for y in ACS_YEARS for s in ALL_ACS_TABLES], columns=["year", "source_table"])
    out = expected.merge(df, on=["year", "source_table"], how="left")
    out["row_count"] = out["row_count"].fillna(0).astype(int)
    out["is_present"] = out["row_count"] > 0
    out["batch_group"] = out["source_table"].map(batch_group_for_source)
    return out.sort_values(["year", "batch_group", "source_table"]).reset_index(drop=True)



def _bridge_coverage_by_year(conn, schema: str, table: str, year_col: str | None, geoid_col: str | None) -> pd.DataFrame:
    bridge_geoid_col, bridge_year_col = bridge_columns(conn, schema, BRIDGE_TABLE)
    if not all([year_col, geoid_col, bridge_geoid_col, bridge_year_col]):
        return pd.DataFrame()

    frames = []
    for year in ACS_YEARS:
        sql = f"""
            WITH src AS (
                SELECT DISTINCT {qident(geoid_col)}::text AS tract_geoid
                FROM {qtable(schema, table)}
                WHERE {qident(year_col)} = %s
                  AND NULLIF(BTRIM({qident(geoid_col)}::text), '') IS NOT NULL
            ),
            br AS (
                SELECT DISTINCT {qident(bridge_geoid_col)}::text AS tract_geoid
                FROM {qtable(schema, BRIDGE_TABLE)}
                WHERE {qident(bridge_year_col)} = %s
                  AND NULLIF(BTRIM({qident(bridge_geoid_col)}::text), '') IS NOT NULL
            )
            SELECT
                %s AS year,
                (SELECT COUNT(*) FROM src) AS src_distinct_geoid_count,
                (SELECT COUNT(*) FROM br) AS bridge_distinct_geoid_count,
                (SELECT COUNT(*) FROM src s INNER JOIN br b ON s.tract_geoid = b.tract_geoid) AS matched_geoid_count,
                (SELECT COUNT(*) FROM src s LEFT JOIN br b ON s.tract_geoid = b.tract_geoid WHERE b.tract_geoid IS NULL) AS missing_in_bridge_geoid_count,
                (SELECT COUNT(*) FROM br b LEFT JOIN src s ON s.tract_geoid = b.tract_geoid WHERE s.tract_geoid IS NULL) AS missing_in_source_geoid_count
        """
        frames.append(read_sql(conn, sql, params=(year, year, year)))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()



def validate_fact_tables() -> None:
    settings = load_settings()
    output_dir = ensure_dir(settings.output_root / OUTPUT_SUBDIR)
    summary_dir = ensure_dir(settings.output_root / "summary")

    log("Starting fact-table validation")

    with get_connection(settings) as conn:
        table_presence = pd.DataFrame(
            {
                "table_name": [LONG_FACT, PROFILE_FACT],
                "exists": [
                    table_exists(conn, settings.schema, LONG_FACT),
                    table_exists(conn, settings.schema, PROFILE_FACT),
                ],
            }
        )
        write_csv(table_presence, output_dir / "fact_table_presence.csv")

        summary_rows: list[dict] = []

        # ---------------------------------------------------------------
        # Long fact validation
        # ---------------------------------------------------------------
        if table_exists(conn, settings.schema, LONG_FACT):
            col_map = detect_table_columns(conn, settings.schema, LONG_FACT)
            year_col = col_map["year_col"]
            geoid_col = col_map["geoid_col"]
            source_col = col_map["source_table_col"]
            metric_name_col = col_map["metric_name_col"]
            metric_value_col = col_map["metric_value_col"]

            long_row_count = row_count(conn, settings.schema, LONG_FACT)
            long_distinct_geoid_count = distinct_nonmissing_count(conn, settings.schema, LONG_FACT, geoid_col) if geoid_col else None
            long_null_geoid_count = null_count(conn, settings.schema, LONG_FACT, geoid_col) if geoid_col else None
            long_blank_geoid_count = blank_count(conn, settings.schema, LONG_FACT, geoid_col) if geoid_col else None

            duplicate_key_cols = [col for col in [year_col, geoid_col, source_col, metric_name_col] if col]
            dup_groups, dup_excess_rows = duplicate_stats(conn, settings.schema, LONG_FACT, duplicate_key_cols)

            key_missing_cols = [col for col in [year_col, geoid_col, source_col, metric_name_col, metric_value_col] if col]
            key_missing_df = missing_profile_df(conn, settings.schema, LONG_FACT, key_missing_cols)
            if not key_missing_df.empty:
                key_missing_df["missing_pct"] = (key_missing_df["missing_count"] / key_missing_df["row_count"]).round(6)
            write_csv(key_missing_df, output_dir / "fact_metric_long_key_missing_profile.csv")

            if year_col:
                long_year_counts = year_counts_df(conn, settings.schema, LONG_FACT, year_col)
            else:
                long_year_counts = pd.DataFrame()
            write_csv(long_year_counts, output_dir / "fact_metric_long_year_counts.csv")

            if year_col and source_col:
                long_source_year_counts = source_year_counts_df(conn, settings.schema, LONG_FACT, year_col, source_col)
                coverage_flags = _make_coverage_flags(long_source_year_counts)
                batch_coverage = (
                    coverage_flags.groupby(["year", "batch_group"], dropna=False)["is_present"]
                    .agg(expected_source_count="size", present_source_count="sum")
                    .reset_index()
                )
                batch_coverage["all_sources_present"] = batch_coverage["expected_source_count"] == batch_coverage["present_source_count"]
            else:
                long_source_year_counts = pd.DataFrame()
                coverage_flags = pd.DataFrame()
                batch_coverage = pd.DataFrame()
            write_csv(long_source_year_counts, output_dir / "fact_metric_long_year_source_counts.csv")
            write_csv(coverage_flags, output_dir / "fact_metric_long_coverage_flags.csv")
            write_csv(batch_coverage, output_dir / "fact_metric_long_batch_coverage.csv")

            numeric_profile_long = numeric_profile_df(conn, settings.schema, LONG_FACT, [metric_value_col]) if metric_value_col else pd.DataFrame()
            write_csv(numeric_profile_long, output_dir / "fact_metric_long_numeric_profile.csv")

            bridge_df = _bridge_coverage_by_year(conn, settings.schema, LONG_FACT, year_col, geoid_col)
            write_csv(bridge_df, output_dir / "fact_metric_long_bridge_coverage_by_year.csv")

            long_key_checks = pd.DataFrame(
                [
                    {
                        "table_name": LONG_FACT,
                        "row_count": long_row_count,
                        "distinct_geoid_count": long_distinct_geoid_count,
                        "null_geoid_count": long_null_geoid_count,
                        "blank_geoid_count": long_blank_geoid_count,
                        "duplicate_key_groups": dup_groups,
                        "duplicate_key_excess_rows": dup_excess_rows,
                        "year_col": year_col,
                        "geoid_col": geoid_col,
                        "source_table_col": source_col,
                        "metric_name_col": metric_name_col,
                        "metric_value_col": metric_value_col,
                    }
                ]
            )
            write_csv(long_key_checks, output_dir / "fact_metric_long_key_checks.csv")

            summary_rows.extend(
                [
                    {"layer": "fact", "metric": "fact_metric_long_exists", "value": 1},
                    {"layer": "fact", "metric": "fact_metric_long_row_count", "value": int(long_row_count)},
                    {"layer": "fact", "metric": "fact_metric_long_duplicate_key_groups", "value": int(dup_groups)},
                ]
            )
        else:
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_key_missing_profile.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_year_counts.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_year_source_counts.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_coverage_flags.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_batch_coverage.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_numeric_profile.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_bridge_coverage_by_year.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_metric_long_key_checks.csv")
            summary_rows.append({"layer": "fact", "metric": "fact_metric_long_exists", "value": 0})

        # ---------------------------------------------------------------
        # Curated profile fact validation
        # ---------------------------------------------------------------
        if table_exists(conn, settings.schema, PROFILE_FACT):
            col_map = detect_table_columns(conn, settings.schema, PROFILE_FACT)
            year_col = col_map["year_col"]
            geoid_col = col_map["geoid_col"]

            profile_row_count = row_count(conn, settings.schema, PROFILE_FACT)
            profile_distinct_geoid_count = distinct_nonmissing_count(conn, settings.schema, PROFILE_FACT, geoid_col) if geoid_col else None
            profile_null_geoid_count = null_count(conn, settings.schema, PROFILE_FACT, geoid_col) if geoid_col else None
            profile_blank_geoid_count = blank_count(conn, settings.schema, PROFILE_FACT, geoid_col) if geoid_col else None
            dup_groups, dup_excess_rows = duplicate_stats(conn, settings.schema, PROFILE_FACT, [col for col in [year_col, geoid_col] if col])

            key_missing_cols = [col for col in [year_col, geoid_col] if col]
            key_missing_df = missing_profile_df(conn, settings.schema, PROFILE_FACT, key_missing_cols)
            if not key_missing_df.empty:
                key_missing_df["missing_pct"] = (key_missing_df["missing_count"] / key_missing_df["row_count"]).round(6)
            write_csv(key_missing_df, output_dir / "fact_profile_key_missing_profile.csv")

            profile_year_counts = year_counts_df(conn, settings.schema, PROFILE_FACT, year_col) if year_col else pd.DataFrame()
            write_csv(profile_year_counts, output_dir / "fact_profile_year_counts.csv")

            profile_bridge_df = _bridge_coverage_by_year(conn, settings.schema, PROFILE_FACT, year_col, geoid_col)
            write_csv(profile_bridge_df, output_dir / "fact_profile_bridge_coverage_by_year.csv")

            numeric_cols = [col for col in get_numeric_columns(conn, settings.schema, PROFILE_FACT) if col != year_col]
            profile_numeric_df = numeric_profile_df(conn, settings.schema, PROFILE_FACT, numeric_cols)
            write_csv(profile_numeric_df, output_dir / "fact_profile_numeric_profile.csv")

            profile_key_checks = pd.DataFrame(
                [
                    {
                        "table_name": PROFILE_FACT,
                        "row_count": profile_row_count,
                        "distinct_geoid_count": profile_distinct_geoid_count,
                        "null_geoid_count": profile_null_geoid_count,
                        "blank_geoid_count": profile_blank_geoid_count,
                        "duplicate_key_groups": dup_groups,
                        "duplicate_key_excess_rows": dup_excess_rows,
                        "year_col": year_col,
                        "geoid_col": geoid_col,
                        "numeric_column_count": len(numeric_cols),
                    }
                ]
            )
            write_csv(profile_key_checks, output_dir / "fact_profile_key_checks.csv")

            summary_rows.extend(
                [
                    {"layer": "fact", "metric": "fact_profile_exists", "value": 1},
                    {"layer": "fact", "metric": "fact_profile_row_count", "value": int(profile_row_count)},
                    {"layer": "fact", "metric": "fact_profile_duplicate_key_groups", "value": int(dup_groups)},
                ]
            )
        else:
            write_csv(pd.DataFrame(), output_dir / "fact_profile_key_missing_profile.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_profile_year_counts.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_profile_bridge_coverage_by_year.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_profile_numeric_profile.csv")
            write_csv(pd.DataFrame(), output_dir / "fact_profile_key_checks.csv")
            summary_rows.append({"layer": "fact", "metric": "fact_profile_exists", "value": 0})

    write_run_summary(summary_rows, summary_dir / "fact_validation_run_summary.csv")
    log("Completed fact-table validation")


if __name__ == "__main__":
    try:
        validate_fact_tables()
    except Exception as exc:  # pragma: no cover
        cli_error(exc)
