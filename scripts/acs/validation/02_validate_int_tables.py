from __future__ import annotations

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
    get_numeric_columns,
    get_text_like_columns,
    load_settings,
    log,
    missing_profile_df,
    null_count,
    numeric_profile_df,
    parse_year_and_source,
    row_count,
    write_csv,
    write_run_summary,
    discover_tables,
)


OUTPUT_SUBDIR = "int"


def validate_int_tables() -> None:
    settings = load_settings()
    output_dir = ensure_dir(settings.output_root / OUTPUT_SUBDIR)
    summary_dir = ensure_dir(settings.output_root / "summary")

    log("Starting intermediate-table validation")

    with get_connection(settings) as conn:
        discovered_tables = discover_tables(conn, settings.schema, "int_acs_%")
        manifest = add_exists_flag(expected_table_manifest("int"), discovered_tables)
        write_csv(manifest, output_dir / "int_expected_manifest.csv")

        table_count_rows: list[dict] = []
        numeric_profile_frames: list[pd.DataFrame] = []
        key_missing_frames: list[pd.DataFrame] = []

        for table_name in discovered_tables:
            year, source_table = parse_year_and_source(table_name)
            col_map = detect_table_columns(conn, settings.schema, table_name)
            geoid_col = col_map["geoid_col"]
            year_col = col_map["year_col"]

            total_rows = row_count(conn, settings.schema, table_name)
            distinct_geoid_count = distinct_nonmissing_count(conn, settings.schema, table_name, geoid_col) if geoid_col else None
            null_geoid_count = null_count(conn, settings.schema, table_name, geoid_col) if geoid_col else None
            blank_geoid_count = blank_count(conn, settings.schema, table_name, geoid_col) if geoid_col else None

            duplicate_key_cols = [col for col in [year_col, geoid_col] if col] or ([geoid_col] if geoid_col else [])
            duplicate_groups, duplicate_excess_rows = duplicate_stats(conn, settings.schema, table_name, duplicate_key_cols)
            bridge_stats = bridge_coverage(conn, settings.schema, table_name, geoid_col, year)

            numeric_columns = [col for col in get_numeric_columns(conn, settings.schema, table_name) if col not in {year_col}]
            if numeric_columns:
                numeric_df = numeric_profile_df(conn, settings.schema, table_name, numeric_columns)
                numeric_df.insert(0, "table_name", table_name)
                numeric_df.insert(1, "year", year)
                numeric_df.insert(2, "source_table", source_table)
                numeric_df.insert(3, "batch_group", batch_group_for_source(source_table))
                numeric_profile_frames.append(numeric_df)

            key_candidates = [col for col in [geoid_col, year_col] if col]
            if key_candidates:
                key_missing_df = missing_profile_df(conn, settings.schema, table_name, key_candidates)
                key_missing_df.insert(0, "table_name", table_name)
                key_missing_df.insert(1, "year", year)
                key_missing_df.insert(2, "source_table", source_table)
                key_missing_df["missing_pct"] = (key_missing_df["missing_count"] / key_missing_df["row_count"]).round(6)
                key_missing_frames.append(key_missing_df)

            table_count_rows.append(
                {
                    "table_name": table_name,
                    "year": year,
                    "source_table": source_table,
                    "batch_group": batch_group_for_source(source_table),
                    "year_col": year_col,
                    "geoid_col": geoid_col,
                    "row_count": total_rows,
                    "distinct_geoid_count": distinct_geoid_count,
                    "null_geoid_count": null_geoid_count,
                    "blank_geoid_count": blank_geoid_count,
                    "duplicate_key_groups": duplicate_groups,
                    "duplicate_key_excess_rows": duplicate_excess_rows,
                    **bridge_stats,
                    "numeric_column_count": len(numeric_columns),
                    "text_column_count": len(get_text_like_columns(conn, settings.schema, table_name)),
                }
            )

            log(f"Validated int table: {table_name}")

    counts_df = pd.DataFrame(table_count_rows).sort_values(["year", "source_table", "table_name"]) if table_count_rows else pd.DataFrame()
    write_csv(counts_df, output_dir / "int_table_counts.csv")

    if numeric_profile_frames:
        numeric_profiles_df = pd.concat(numeric_profile_frames, ignore_index=True)
        write_csv(numeric_profiles_df, output_dir / "int_numeric_profile.csv")
    else:
        write_csv(pd.DataFrame(), output_dir / "int_numeric_profile.csv")

    if key_missing_frames:
        key_missing_df = pd.concat(key_missing_frames, ignore_index=True)
        write_csv(key_missing_df, output_dir / "int_key_missing_profile.csv")
    else:
        write_csv(pd.DataFrame(), output_dir / "int_key_missing_profile.csv")

    batch_coverage = (
        manifest.groupby(["year", "batch_group"], dropna=False)["exists"]
        .agg(expected_table_count="size", existing_table_count="sum")
        .reset_index()
    )
    batch_coverage["all_expected_tables_present"] = batch_coverage["expected_table_count"] == batch_coverage["existing_table_count"]
    write_csv(batch_coverage, output_dir / "int_batch_coverage.csv")

    run_summary = [
        {
            "layer": "int",
            "metric": "expected_table_count",
            "value": int(manifest.shape[0]),
        },
        {
            "layer": "int",
            "metric": "discovered_table_count",
            "value": int(len(discovered_tables)),
        },
        {
            "layer": "int",
            "metric": "missing_table_count",
            "value": int((~manifest["exists"]).sum()),
        },
        {
            "layer": "int",
            "metric": "tables_with_duplicate_keys",
            "value": int((counts_df["duplicate_key_groups"].fillna(0) > 0).sum()) if not counts_df.empty else 0,
        },
        {
            "layer": "int",
            "metric": "tables_with_bridge_mismatch",
            "value": int((counts_df["missing_in_bridge_geoid_count"].fillna(0) > 0).sum()) if not counts_df.empty else 0,
        },
    ]
    write_run_summary(run_summary, summary_dir / "int_validation_run_summary.csv")

    log("Completed intermediate-table validation")


if __name__ == "__main__":
    try:
        validate_int_tables()
    except Exception as exc:  # pragma: no cover
        cli_error(exc)
