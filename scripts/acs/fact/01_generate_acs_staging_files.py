from __future__ import annotations

from pathlib import Path
import re
import pandas as pd

YEARS = [2019, 2021, 2022, 2023]
STATEFP_FILTER = "17"
COUNTYFP_FILTER = "019"


def find_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cleaned = []
    for c in df.columns:
        c = str(c).strip()
        c = re.sub(r"\s+", "_", c)
        c = c.replace("-", "_")
        cleaned.append(c)
    df.columns = cleaned
    return df


def get_required_column(df: pd.DataFrame, col_name: str) -> str:
    for col in df.columns:
        if col.upper() == col_name.upper():
            return col
    raise KeyError(f"Required column '{col_name}' not found.")


def parse_geo_id(geo_id: str) -> tuple[str, str, str, str]:
    geo_id = str(geo_id).strip()
    tract_geoid = geo_id.replace("1400000US", "")
    statefp = tract_geoid[0:2]
    countyfp = tract_geoid[2:5]
    tractce = tract_geoid[5:11]
    return tract_geoid, statefp, countyfp, tractce


def build_source_file_name(source_type: str, year: int, table_code: str) -> str:
    return f"ACS{source_type}5Y{year}.{table_code}-Data.csv"


def main() -> None:
    project_root = find_project_root()

    inventory_file = project_root / "outputs" / "acs" / "inventory" / "acs_folder_year_summary.csv"
    if not inventory_file.exists():
        raise FileNotFoundError(f"Inventory file not found: {inventory_file}")

    raw_root_candidates = [
        project_root / "data" / "raw",
        project_root / "raw",
        project_root / "data" / "raw" / "acs",
        project_root / "data" / "acs",
    ]
    raw_root = next((p for p in raw_root_candidates if p.exists()), None)
    if raw_root is None:
        raise FileNotFoundError("Could not find ACS raw root folder.")

    processed_dir = project_root / "data" / "acs" / "processed" / "acs_tract" / "staging"
    summary_dir = project_root / "outputs" / "acs" / "acs_tract"
    processed_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)

    inv = pd.read_csv(inventory_file, dtype=str)

    # only rows where Data.csv exists and expected year is in pipeline
    inv["year"] = inv["year"].astype(int)
    inv = inv[
        (inv["year"].isin(YEARS)) &
        (inv["has_data_csv"].astype(str).str.lower() == "true")
    ].copy()

    inv = inv[
        ["folder_name", "table_code", "table_family", "source_type", "year"]
    ].drop_duplicates().sort_values(["year", "table_code"]).reset_index(drop=True)

    summary_rows = []

    for _, row in inv.iterrows():
        folder_name = row["folder_name"]
        table_code = row["table_code"]
        table_family = row["table_family"]
        source_type = row["source_type"]
        year = int(row["year"])

        source_file = raw_root / folder_name / build_source_file_name(source_type, year, table_code)
        if not source_file.exists():
            raise FileNotFoundError(f"Missing source file: {source_file}")

        df = pd.read_csv(source_file, dtype=str)
        df = clean_columns(df)

        geo_col = get_required_column(df, "GEO_ID")
        name_col = get_required_column(df, "NAME")

        # keep tract rows only
        df = df[df[geo_col].astype(str).str.startswith("1400000US")].copy()

        geo_parts = df[geo_col].apply(parse_geo_id)
        df["tract_geoid"] = geo_parts.apply(lambda x: x[0])
        df["statefp"] = geo_parts.apply(lambda x: x[1])
        df["countyfp"] = geo_parts.apply(lambda x: x[2])
        df["tractce"] = geo_parts.apply(lambda x: x[3])

        # filter to Champaign County, Illinois
        df = df[
            (df["statefp"] == STATEFP_FILTER) &
            (df["countyfp"] == COUNTYFP_FILTER)
        ].copy()

        df["year"] = year
        df["table_code"] = table_code
        df["table_family"] = table_family
        df["source_type"] = source_type
        df["geo_id"] = df[geo_col].astype(str).str.strip()
        df["name"] = df[name_col].astype(str).str.strip()

        front_cols = [
            "year",
            "table_code",
            "table_family",
            "source_type",
            "geo_id",
            "name",
            "tract_geoid",
            "statefp",
            "countyfp",
            "tractce",
        ]
        remaining_cols = [c for c in df.columns if c not in front_cols]
        df = df[front_cols + remaining_cols].drop_duplicates().reset_index(drop=True)

        year_dir = processed_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        out_file = year_dir / f"stg_acs_{year}_{table_code.lower()}_raw.csv"
        df.to_csv(out_file, index=False)

        summary_rows.append(
            {
                "year": year,
                "table_code": table_code,
                "table_family": table_family,
                "source_type": source_type,
                "row_count": len(df),
                "column_count": len(df.columns),
                "output_file": str(out_file),
            }
        )

        print(f"[OK] {year} {table_code}: {len(df)} rows, {len(df.columns)} columns -> {out_file}")

    summary_df = pd.DataFrame(summary_rows).sort_values(["year", "table_code"]).reset_index(drop=True)
    summary_file = summary_dir / "all_acs_staging_summary.csv"
    summary_df.to_csv(summary_file, index=False)

    print("\nDone.")
    print(f"[OK] Summary file: {summary_file}")


if __name__ == "__main__":
    main()