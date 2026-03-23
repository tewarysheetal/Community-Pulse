from __future__ import annotations

from pathlib import Path
import pandas as pd


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def main() -> None:
    project_root = get_project_root()

    audit_file = (
        project_root
        / "data"
        / "acs"
        / "processed"
        / "acs_tract"
        / "dimensions"
        / "dim_tract_year_audit.csv"
    )

    if not audit_file.exists():
        raise FileNotFoundError(f"Missing audit file: {audit_file}")

    df = pd.read_csv(audit_file, dtype=str)

    for col in ["tract_geoid", "geo_id", "tractce", "tract_name", "year"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    output_dir = project_root / "outputs" / "acs" / "acs_tract"
    output_dir.mkdir(parents=True, exist_ok=True)

    # GEOID validation
    geoid_check = df[["year", "tract_geoid", "geo_id", "tractce", "tract_name"]].drop_duplicates()
    geoid_check["tract_geoid_len"] = geoid_check["tract_geoid"].str.len()
    geoid_check["tract_geoid_is_digits"] = geoid_check["tract_geoid"].str.isdigit()
    geoid_check["tractce_len"] = geoid_check["tractce"].str.len()
    geoid_check["tractce_is_digits"] = geoid_check["tractce"].str.isdigit()

    bad_geoids = geoid_check[
        (geoid_check["tract_geoid_len"] != 11)
        | (~geoid_check["tract_geoid_is_digits"])
        | (geoid_check["tractce_len"] != 6)
        | (~geoid_check["tractce_is_digits"])
    ].sort_values(["year", "tract_geoid"])

    # Year presence
    year_presence = (
        df.groupby("tract_geoid")
        .agg(
            tract_name_example=("tract_name", "first"),
            years_present=("year", lambda x: ",".join(sorted(set(x)))),
            year_count=("year", "nunique"),
        )
        .reset_index()
        .sort_values(["year_count", "tract_geoid"], ascending=[True, True])
    )

    inconsistent_presence = year_presence[year_presence["year_count"] < 4].copy()

    # Name conflicts
    name_counts = (
        df.groupby("tract_geoid")["tract_name"]
        .nunique()
        .reset_index(name="distinct_name_count")
    )

    conflict_geoids = name_counts[name_counts["distinct_name_count"] > 1]["tract_geoid"]

    name_conflict_details = (
        df[df["tract_geoid"].isin(conflict_geoids)]
        [["year", "tract_geoid", "tract_name", "geo_id", "tractce"]]
        .drop_duplicates()
        .sort_values(["tract_geoid", "year"])
    )

    bad_geoids.to_csv(output_dir / "dim_tract_bad_geoids.csv", index=False)
    year_presence.to_csv(output_dir / "dim_tract_year_presence.csv", index=False)
    inconsistent_presence.to_csv(output_dir / "dim_tract_inconsistent_presence.csv", index=False)
    name_conflict_details.to_csv(output_dir / "dim_tract_name_conflict_details.csv", index=False)

    print("=" * 80)
    print("DIM_TRACT VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Total distinct tract_geoid across all years: {df['tract_geoid'].nunique()}")
    print()

    print("Distinct tract count by year:")
    print(
        df.groupby("year")["tract_geoid"]
        .nunique()
        .reset_index(name="tract_count")
        .to_string(index=False)
    )
    print()

    print(f"Bad GEOID rows: {len(bad_geoids)}")
    print(f"Tracts not present in all 4 years: {len(inconsistent_presence)}")
    print(f"Tracts with name conflicts: {name_conflict_details['tract_geoid'].nunique()}")
    print()

    print("Output files:")
    print(output_dir / "dim_tract_bad_geoids.csv")
    print(output_dir / "dim_tract_year_presence.csv")
    print(output_dir / "dim_tract_inconsistent_presence.csv")
    print(output_dir / "dim_tract_name_conflict_details.csv")


if __name__ == "__main__":
    main()