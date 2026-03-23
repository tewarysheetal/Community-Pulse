from __future__ import annotations

from pathlib import Path
import re
import pandas as pd

# ============================================================
# Build dim_tract from ACS tract-level files
# ------------------------------------------------------------
# Uses B19013 files from 2019, 2021, 2022, 2023 as geography source
#
# Outputs:
#   data/acs/processed/acs_tract/dimensions/dim_tract.csv
#   data/acs/processed/acs_tract/dimensions/dim_tract_year_audit.csv
#   outputs/acs/acs_tract/dim_tract_year_check.csv
#   outputs/acs/acs_tract/dim_tract_name_conflicts.csv
# ============================================================

YEARS = [2019, 2021, 2022, 2023]
STATEFP_FILTER = "17"
COUNTYFP_FILTER = "019"

SOURCE_FOLDER = "MedianHouseholdIncome(B19013)_2023_2019"

CANDIDATE_RAW_ROOTS = [
    "data/raw",
    "raw",
    "data/raw/acs",
    "data/acs",
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


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df


def get_required_column(df: pd.DataFrame, col_name: str) -> str:
    for col in df.columns:
        if col.upper() == col_name.upper():
            return col
    raise KeyError(f"Required column '{col_name}' not found.")


def parse_geo_id(geo_id: str) -> tuple[str, str, str, str]:
    """
    Example:
      GEO_ID = 1400000US17019000100

    Returns:
      tract_geoid = 17019000100
      statefp     = 17
      countyfp    = 019
      tractce     = 000100
    """
    geo_id = str(geo_id).strip()
    tract_geoid = geo_id.replace("1400000US", "")
    statefp = tract_geoid[0:2]
    countyfp = tract_geoid[2:5]
    tractce = tract_geoid[5:11]
    return tract_geoid, statefp, countyfp, tractce


def extract_tract_number(name: str) -> str | None:
    if pd.isna(name):
        return None
    match = re.search(r"Census Tract\s+([^,]+)", str(name))
    return match.group(1).strip() if match else None


def get_source_file(raw_root: Path, year: int) -> Path:
    return raw_root / SOURCE_FOLDER / f"ACSDT5Y{year}.B19013-Data.csv"


def extract_year_tracts(raw_root: Path, year: int) -> pd.DataFrame:
    source_file = get_source_file(raw_root, year)
    if not source_file.exists():
        raise FileNotFoundError(f"Missing source file: {source_file}")

    df = pd.read_csv(source_file, dtype=str)
    df = clean_columns(df)

    geo_col = get_required_column(df, "GEO_ID")
    name_col = get_required_column(df, "NAME")

    # Keep tract rows only
    df = df[df[geo_col].astype(str).str.startswith("1400000US")].copy()

    geo_parts = df[geo_col].apply(parse_geo_id)
    df["tract_geoid"] = geo_parts.apply(lambda x: x[0])
    df["statefp"] = geo_parts.apply(lambda x: x[1])
    df["countyfp"] = geo_parts.apply(lambda x: x[2])
    df["tractce"] = geo_parts.apply(lambda x: x[3])

    # Filter to Champaign County, IL
    df = df[
        (df["statefp"] == STATEFP_FILTER) &
        (df["countyfp"] == COUNTYFP_FILTER)
    ].copy()

    df["year"] = year
    df["geo_id"] = df[geo_col].astype(str).str.strip()
    df["tract_name"] = df[name_col].astype(str).str.strip()
    df["tract_number"] = df["tract_name"].apply(extract_tract_number)
    df["county_name"] = "Champaign County"
    df["state_name"] = "Illinois"

    return df[
        [
            "year",
            "geo_id",
            "tract_geoid",
            "statefp",
            "countyfp",
            "tractce",
            "tract_number",
            "tract_name",
            "county_name",
            "state_name",
        ]
    ].drop_duplicates()


def build_dim_tract(audit_df: pd.DataFrame) -> pd.DataFrame:
    dim_tract = (
        audit_df.sort_values(["tract_geoid", "year"])
        .groupby("tract_geoid", as_index=False)
        .agg(
            geo_id=("geo_id", "first"),
            statefp=("statefp", "first"),
            countyfp=("countyfp", "first"),
            tractce=("tractce", "first"),
            tract_number=("tract_number", "first"),
            tract_name=("tract_name", "first"),
            county_name=("county_name", "first"),
            state_name=("state_name", "first"),
            first_year_seen=("year", "min"),
            last_year_seen=("year", "max"),
            year_count=("year", "nunique"),
        )
        .sort_values("tractce")
        .reset_index(drop=True)
    )

    return dim_tract[
        [
            "tract_geoid",
            "geo_id",
            "statefp",
            "countyfp",
            "tractce",
            "tract_number",
            "tract_name",
            "county_name",
            "state_name",
            "first_year_seen",
            "last_year_seen",
            "year_count",
        ]
    ]


def build_year_check(audit_df: pd.DataFrame) -> pd.DataFrame:
    return (
        audit_df.groupby("year")
        .agg(
            tract_count=("tract_geoid", "nunique"),
            min_tractce=("tractce", "min"),
            max_tractce=("tractce", "max"),
        )
        .reset_index()
        .sort_values("year")
    )


def build_name_conflicts(audit_df: pd.DataFrame) -> pd.DataFrame:
    name_counts = (
        audit_df.groupby("tract_geoid")["tract_name"]
        .nunique()
        .reset_index(name="distinct_name_count")
    )

    conflict_geoids = name_counts[name_counts["distinct_name_count"] > 1]["tract_geoid"]

    conflicts = (
        audit_df[audit_df["tract_geoid"].isin(conflict_geoids)]
        .sort_values(["tract_geoid", "year"])
        .reset_index(drop=True)
    )

    return conflicts


def main() -> None:
    project_root = find_project_root()
    raw_root = find_raw_root(project_root)

    processed_dir = project_root / "data" / "acs" / "processed" / "acs_tract" / "dimensions"
    output_dir = project_root / "outputs" / "acs" / "acs_tract"

    processed_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    year_frames: list[pd.DataFrame] = []

    for year in YEARS:
        df_year = extract_year_tracts(raw_root, year)
        year_frames.append(df_year)
        print(f"[OK] {year}: {df_year['tract_geoid'].nunique()} tracts")

    audit_df = pd.concat(year_frames, ignore_index=True).drop_duplicates()

    dim_tract = build_dim_tract(audit_df)
    year_check = build_year_check(audit_df)
    name_conflicts = build_name_conflicts(audit_df)

    dim_tract_file = processed_dir / "dim_tract.csv"
    audit_file = processed_dir / "dim_tract_year_audit.csv"
    year_check_file = output_dir / "dim_tract_year_check.csv"
    name_conflicts_file = output_dir / "dim_tract_name_conflicts.csv"

    dim_tract.to_csv(dim_tract_file, index=False)
    audit_df.to_csv(audit_file, index=False)
    year_check.to_csv(year_check_file, index=False)
    name_conflicts.to_csv(name_conflicts_file, index=False)

    print("\nDone.")
    print(f"[OK] dim_tract rows: {len(dim_tract)}")
    print(f"[OK] dim_tract file: {dim_tract_file}")
    print(f"[OK] audit file: {audit_file}")
    print(f"[OK] year check file: {year_check_file}")
    print(f"[OK] name conflicts file: {name_conflicts_file}")

    if name_conflicts.empty:
        print("[OK] No tract naming conflicts across years.")
    else:
        print("[WARN] Some tracts have different names across years. Check the conflicts file.")


if __name__ == "__main__":
    main()