from __future__ import annotations

from pathlib import Path
import re
import pandas as pd

# ============================================================
# Extract tract rows from ACS files and build tract dimension CSVs
# ------------------------------------------------------------
# Source used:
#   MedianHouseholdIncome(B19013)_2023_2019
#   -> ACSDT5Y{year}.B19013-Data.csv
#
# Years:
#   2019, 2021, 2022, 2023
#
# Filters:
#   Illinois      = statefp '17'
#   Champaign     = countyfp '019'
#   Tract summary = GEO_ID starts with '1400000US'
#
# Outputs:
#   data/processed/acs/tract/dim_tract_2019.csv
#   data/processed/acs/tract/dim_tract_2021.csv
#   data/processed/acs/tract/dim_tract_2022.csv
#   data/processed/acs/tract/dim_tract_2023.csv
#   data/processed/acs/tract/dim_tract_all_years.csv
#   outputs/acs/tract/dim_tract_summary.csv
# ============================================================

YEARS = [2019, 2021, 2022, 2023]

STATEFP_FILTER = "17"
COUNTYFP_FILTER = "019"

SOURCE_FOLDER = "MedianHouseholdIncome(B19013)_2023_2019"

# Adjust only if your project structure changes
CANDIDATE_RAW_ROOTS = [
    "data/raw",
    "raw",
    "data/raw/acs",
    "data/acs"
]


def find_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def find_raw_root(project_root: Path) -> Path:
    for rel_path in CANDIDATE_RAW_ROOTS:
        candidate = project_root / rel_path
        if candidate.exists() and candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "Could not find ACS raw root. Checked: "
        + ", ".join(str(project_root / p) for p in CANDIDATE_RAW_ROOTS)
    )


def find_geo_column(columns: list[str]) -> str:
    for col in columns:
        if col.upper() == "GEO_ID":
            return col
    raise KeyError("Could not find GEO_ID column.")


def find_name_column(columns: list[str]) -> str:
    for col in columns:
        if col.upper() == "NAME":
            return col
    raise KeyError("Could not find NAME column.")


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df


def extract_geoid_parts(geo_id: str) -> tuple[str, str, str, str]:
    """
    Example:
        GEO_ID = 1400000US17019000900

    Returns:
        tract_geoid = 17019000900
        statefp     = 17
        countyfp    = 019
        tractce     = 000900
    """
    geo_id = str(geo_id).strip()
    tract_geoid = geo_id.replace("1400000US", "")
    statefp = tract_geoid[0:2]
    countyfp = tract_geoid[2:5]
    tractce = tract_geoid[5:11]
    return tract_geoid, statefp, countyfp, tractce


def extract_tract_number_from_name(name: str) -> str | None:
    """
    Example:
        'Census Tract 1, Champaign County, Illinois'
        -> '1'

        'Census Tract 53.01, Champaign County, Illinois'
        -> '53.01'
    """
    if pd.isna(name):
        return None
    match = re.search(r"Census Tract\s+([^,]+)", str(name))
    if match:
        return match.group(1).strip()
    return None


def build_dim_tract_for_year(raw_root: Path, year: int) -> pd.DataFrame:
    source_file = (
        raw_root
        / SOURCE_FOLDER
        / f"ACSDT5Y{year}.B19013-Data.csv"
    )

    if not source_file.exists():
        raise FileNotFoundError(f"Missing source file: {source_file}")

    df = pd.read_csv(source_file, dtype=str)
    df = standardize_columns(df)

    geo_col = find_geo_column(df.columns.tolist())
    name_col = find_name_column(df.columns.tolist())

    # Keep only tract rows
    df = df[df[geo_col].astype(str).str.startswith("1400000US")].copy()

    # Extract geographic parts
    parts = df[geo_col].apply(extract_geoid_parts)
    df["tract_geoid"] = parts.apply(lambda x: x[0])
    df["statefp"] = parts.apply(lambda x: x[1])
    df["countyfp"] = parts.apply(lambda x: x[2])
    df["tractce"] = parts.apply(lambda x: x[3])

    # Filter to Illinois / Champaign County
    df = df[
        (df["statefp"] == STATEFP_FILTER)
        & (df["countyfp"] == COUNTYFP_FILTER)
    ].copy()

    # Build final tract dimension columns
    df["year"] = year
    df["geo_id"] = df[geo_col].astype(str).str.strip()
    df["name"] = df[name_col].astype(str).str.strip()
    df["tract_number"] = df["name"].apply(extract_tract_number_from_name)
    df["state_name"] = "Illinois"
    df["county_name"] = "Champaign County"

    final_cols = [
        "year",
        "geo_id",
        "tract_geoid",
        "statefp",
        "countyfp",
        "tractce",
        "tract_number",
        "name",
        "state_name",
        "county_name",
    ]

    out = (
        df[final_cols]
        .drop_duplicates()
        .sort_values(["tractce"])
        .reset_index(drop=True)
    )

    return out


def main() -> None:
    project_root = find_project_root()
    raw_root = find_raw_root(project_root)

    processed_dir = project_root / "data"  / "acs" / "processed" / "tract"
    summary_dir = project_root / "outputs" / "acs" / "tract"

    processed_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)

    all_dfs: list[pd.DataFrame] = []
    summary_rows: list[dict] = []

    for year in YEARS:
        dim_df = build_dim_tract_for_year(raw_root, year)

        out_file = processed_dir / f"dim_tract_{year}.csv"
        dim_df.to_csv(out_file, index=False)

        all_dfs.append(dim_df)

        summary_rows.append(
            {
                "year": year,
                "tract_count": len(dim_df),
                "min_tractce": dim_df["tractce"].min() if not dim_df.empty else None,
                "max_tractce": dim_df["tractce"].max() if not dim_df.empty else None,
                "output_file": str(out_file),
            }
        )

        print(f"[OK] {year}: {len(dim_df)} tracts -> {out_file}")

    all_years_df = pd.concat(all_dfs, ignore_index=True)
    all_years_file = processed_dir / "dim_tract_all_years.csv"
    all_years_df.to_csv(all_years_file, index=False)

    summary_df = pd.DataFrame(summary_rows)
    summary_file = summary_dir / "dim_tract_summary.csv"
    summary_df.to_csv(summary_file, index=False)

    print("\nDone.")
    print(f"[OK] Combined file -> {all_years_file}")
    print(f"[OK] Summary file  -> {summary_file}")


if __name__ == "__main__":
    main()