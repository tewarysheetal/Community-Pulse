from __future__ import annotations

from pathlib import Path

import pandas as pd

from validation_utils import (
    add_exists_flag,
    batch_group_for_source,
    blank_count,
    bridge_coverage,
    cli_error,
    detect_table_columns,
    distinct_nonmissing_count,
    duplicate_stats,
    ensure_dir,
    expected_table_manifest,
    get_connection,
    get_text_like_columns,
    load_settings,
    log,
    missing_profile_df,
    null_count,
    parse_year_and_source,
    row_count,
    write_csv,
    write_run_summary,
    discover_tables,
)


OUTPUT_SUBDIR = "stg"


def validate_stg_tables() -> None:
    settings = load_settings()
    output_dir = ensure_dir(settings.output_root / OUTPUT_SUBDIR)
    summary_dir = ensure_dir(settings.output_root / "summary")

    log("Starting staging-table validation")

    with get_connection(settings) as conn:
        discovered_tables = discover_tables(conn, settings.schema, "stg_acs_%_raw")
        manifest = add_exists_flag(expected_table_manifest("stg"), discovered_tables)
        write_csv(manifest, output_dir / "stg_expected_manifest.csv")

        table_count_rows: list[dict] = []
        missing_profile_frames: list[pd.DataFrame] = []

        for table_name in discovered_tables:
            year, source_table = parse_year_and_source(table_name)
            col_map = detect_table_columns(conn, settings.schema, table_name)
            geoid_col = col_map["geoid_col"]

            total_rows = row_count(conn, settings.schema, table_name)
            distinct_geoid_count = distinct_nonmissing_count(conn, settings.schema, table_name, geoid_col) if geoid_col else None
            null_geoid_count = null_count(conn, settings.schema, table_name, geoid_col) if geoid_col else None
            blank_geoid_count = blank_count(conn, settings.schema, table_name, geoid_col) if geoid_col else None
            duplicate_groups, duplicate_excess_rows = duplicate_stats(conn, settings.schema, table_name, [geoid_col] if geoid_col else [])
            bridge_stats = bridge_coverage(conn, settings.schema, table_name, geoid_col, year)

            table_count_rows.append(
                {
                    "table_name": table_name,
                    "year": year,
                    "source_table": source_table,
                    "batch_group": batch_group_for_source(source_table),
                    "geoid_col": geoid_col,
                    "row_count": total_rows,
                    "distinct_geoid_count": distinct_geoid_count,
                    "null_geoid_count": null_geoid_count,
                    "blank_geoid_count": blank_geoid_count,
                    "duplicate_geoid_groups": duplicate_groups,
                    "duplicate_geoid_excess_rows": duplicate_excess_rows,
                    **bridge_stats,
                }
            )

            text_columns = get_text_like_columns(conn, settings.schema, table_name)
            if text_columns:
                missing_df = missing_profile_df(conn, settings.schema, table_name, text_columns)
                missing_df.insert(0, "table_name", table_name)
                missing_df.insert(1, "year", year)
                missing_df.insert(2, "source_table", source_table)
                missing_df["missing_pct"] = (missing_df["missing_count"] / missing_df["row_count"]).round(6)
                missing_profile_frames.append(missing_df)

            log(f"Validated staging table: {table_name}")

    counts_df = pd.DataFrame(table_count_rows).sort_values(["year", "source_table", "table_name"]) if table_count_rows else pd.DataFrame()
    write_csv(counts_df, output_dir / "stg_table_counts.csv")

    if missing_profile_frames:
        missing_profiles_df = pd.concat(missing_profile_frames, ignore_index=True)
        write_csv(missing_profiles_df, output_dir / "stg_missing_profile.csv")
    else:
        write_csv(pd.DataFrame(), output_dir / "stg_missing_profile.csv")

    run_summary = [
        {
            "layer": "stg",
            "metric": "expected_table_count",
            "value": int(manifest.shape[0]),
        },
        {
            "layer": "stg",
            "metric": "discovered_table_count",
            "value": int(len(discovered_tables)),
        },
        {
            "layer": "stg",
            "metric": "missing_table_count",
            "value": int((~manifest["exists"]).sum()),
        },
        {
            "layer": "stg",
            "metric": "tables_with_duplicate_geoids",
            "value": int((counts_df["duplicate_geoid_groups"].fillna(0) > 0).sum()) if not counts_df.empty else 0,
        },
        {
            "layer": "stg",
            "metric": "tables_with_bridge_mismatch",
            "value": int((counts_df["missing_in_bridge_geoid_count"].fillna(0) > 0).sum()) if not counts_df.empty else 0,
        },
    ]
    write_run_summary(run_summary, summary_dir / "stg_validation_run_summary.csv")

    log("Completed staging-table validation")


if __name__ == "__main__":
    try:
        validate_stg_tables()
    except Exception as exc:  # pragma: no cover
        cli_error(exc)
