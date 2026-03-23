from __future__ import annotations

from pathlib import Path
import re
import pandas as pd

# ============================================================
# Final dim_tract + bridge_tract_year builder
# ------------------------------------------------------------
# Uses B19013 files from 2019, 2021, 2022, 2023 as geography source
#
# Final outputs:
#   data/acs/processed/acs_tract/dimensions/dim_tract.csv
#   data/acs/processed/acs_tract/dimensions/bridge_tract_year.csv
#   data/acs/processed/acs_tract/dimensions/dim_tract_year_audit.csv
#
# Validation outputs:
#   outputs/acs/acs_tract/dim_tract_year_check.csv
#   outputs/acs/acs_tract/dim_tract_year_presence.csv
#   outputs/acs/acs_tract/dim_tract_raw_name_conflicts.csv
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
    """
    Example:
      Census Tract 1, Champaign County, Illinois -> 1
      Census Tract 53.01, Champaign County, Illinois -> 53.01
    """
    if pd.isna(name):
        return None
    match = re.search(r"Census Tract\s+([^,;]+)", str(name))
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

    # Filter to Champaign County, Illinois
    df = df[
        (df["statefp"] == STATEFP_FILTER) &
        (df["countyfp"] == COUNTYFP_FILTER)
    ].copy()

    df["year"] = year
    df["geo_id"] = df[geo_col].astype(str).str.strip()
    df["tract_name_raw"] = df[name_col].astype(str).str.strip()
    df["tract_number"] = df["tract_name_raw"].apply(extract_tract_number)
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
            "tract_name_raw",
            "county_name",
            "state_name",
        ]
    ].drop_duplicates()


def build_audit_df(raw_root: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for year in YEARS:
        df_year = extract_year_tracts(raw_root, year)
        frames.append(df_year)
        print(f"[OK] {year}: {df_year['tract_geoid'].nunique()} tracts")

    audit_df = pd.concat(frames, ignore_index=True).drop_duplicates()

    for col in ["tract_geoid", "geo_id", "tractce", "tract_number", "tract_name_raw"]:
        audit_df[col] = audit_df[col].astype(str).str.strip()

    return audit_df.sort_values(["year", "tractce", "tract_geoid"]).reset_index(drop=True)


def choose_canonical_name(group: pd.DataFrame) -> str:
    """
    Prefer latest available raw tract name.
    2023 first, then latest year descending.
    """
    group = group.sort_values("year", ascending=False)

    row_2023 = group[group["year"] == 2023]
    if not row_2023.empty:
        return row_2023.iloc[0]["tract_name_raw"]

    return group.iloc[0]["tract_name_raw"]


def choose_canonical_tract_number(group: pd.DataFrame) -> str | None:
    vals = [v for v in group["tract_number"].tolist() if pd.notna(v) and str(v).strip() not in {"", "nan", "None"}]
    if not vals:
        return None

    # Prefer latest non-null
    latest = (
        group.loc[group["tract_number"].notna()]
        .sort_values("year", ascending=False)
        ["tract_number"]
        .tolist()
    )
    return latest[0] if latest else vals[0]


def build_dim_tract(audit_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []

    for tract_geoid, group in audit_df.groupby("tract_geoid", sort=True):
        group = group.sort_values("year")
        tract_number = choose_canonical_tract_number(group)
        tract_name_latest = choose_canonical_name(group)

        if tract_number and tract_number not in {"nan", "None"}:
            tract_name_canonical = f"Census Tract {tract_number}"
        else:
            tract_name_canonical = tract_name_latest

        rows.append(
            {
                "tract_geoid": tract_geoid,
                "geo_id": group.iloc[-1]["geo_id"],  # latest available GEO_ID
                "statefp": group.iloc[-1]["statefp"],
                "countyfp": group.iloc[-1]["countyfp"],
                "tractce": group.iloc[-1]["tractce"],
                "tract_number": tract_number,
                "tract_name_canonical": tract_name_canonical,
                "tract_name_latest": tract_name_latest,
                "county_name": group.iloc[-1]["county_name"],
                "state_name": group.iloc[-1]["state_name"],
                "first_year_seen": int(group["year"].min()),
                "last_year_seen": int(group["year"].max()),
                "year_count": int(group["year"].nunique()),
                "is_stable_all_4_years": int(group["year"].nunique() == 4),
            }
        )

    dim_tract = pd.DataFrame(rows).sort_values(["tractce", "tract_geoid"]).reset_index(drop=True)
    return dim_tract


def build_bridge_tract_year(audit_df: pd.DataFrame) -> pd.DataFrame:
    bridge = (
        audit_df[
            [
                "year",
                "tract_geoid",
                "geo_id",
                "statefp",
                "countyfp",
                "tractce",
                "tract_number",
                "tract_name_raw",
            ]
        ]
        .drop_duplicates()
        .sort_values(["year", "tractce", "tract_geoid"])
        .reset_index(drop=True)
    )
    return bridge


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


def build_year_presence(dim_tract: pd.DataFrame) -> pd.DataFrame:
    return (
        dim_tract[
            [
                "tract_geoid",
                "tract_number",
                "tract_name_canonical",
                "first_year_seen",
                "last_year_seen",
                "year_count",
                "is_stable_all_4_years",
            ]
        ]
        .sort_values(["year_count", "tract_geoid"])
        .reset_index(drop=True)
    )


def build_raw_name_conflicts(audit_df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        audit_df.groupby("tract_geoid")["tract_name_raw"]
        .nunique()
        .reset_index(name="distinct_raw_name_count")
    )
    conflict_geoids = counts[counts["distinct_raw_name_count"] > 1]["tract_geoid"]

    conflicts = (
        audit_df[audit_df["tract_geoid"].isin(conflict_geoids)]
        .merge(counts, on="tract_geoid", how="left")
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

    audit_df = build_audit_df(raw_root)
    dim_tract = build_dim_tract(audit_df)
    bridge_tract_year = build_bridge_tract_year(audit_df)
    year_check = build_year_check(audit_df)
    year_presence = build_year_presence(dim_tract)
    raw_name_conflicts = build_raw_name_conflicts(audit_df)

    dim_tract_file = processed_dir / "dim_tract.csv"
    bridge_file = processed_dir / "bridge_tract_year.csv"
    audit_file = processed_dir / "dim_tract_year_audit.csv"

    year_check_file = output_dir / "dim_tract_year_check.csv"
    year_presence_file = output_dir / "dim_tract_year_presence.csv"
    raw_name_conflicts_file = output_dir / "dim_tract_raw_name_conflicts.csv"

    dim_tract.to_csv(dim_tract_file, index=False)
    bridge_tract_year.to_csv(bridge_file, index=False)
    audit_df.to_csv(audit_file, index=False)

    year_check.to_csv(year_check_file, index=False)
    year_presence.to_csv(year_presence_file, index=False)
    raw_name_conflicts.to_csv(raw_name_conflicts_file, index=False)

    print("\nDone.")
    print(f"[OK] dim_tract rows: {len(dim_tract)}")
    print(f"[OK] stable all-4-year tracts: {dim_tract['is_stable_all_4_years'].sum()}")
    print(f"[OK] bridge_tract_year rows: {len(bridge_tract_year)}")
    print(f"[OK] raw name conflict tracts: {raw_name_conflicts['tract_geoid'].nunique() if not raw_name_conflicts.empty else 0}")
    print()
    print(f"[OK] dim_tract file: {dim_tract_file}")
    print(f"[OK] bridge file: {bridge_file}")
    print(f"[OK] audit file: {audit_file}")
    print(f"[OK] year check file: {year_check_file}")
    print(f"[OK] year presence file: {year_presence_file}")
    print(f"[OK] raw name conflicts file: {raw_name_conflicts_file}")


if __name__ == "__main__":
    main()