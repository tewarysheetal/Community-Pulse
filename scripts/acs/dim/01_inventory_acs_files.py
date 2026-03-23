from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd

# ============================================================
# ACS File Inventory Script
# ------------------------------------------------------------
# What it does:
# 1. Scans your ACS download folders
# 2. Identifies Data / Column-Metadata / Table-Notes files
# 3. Extracts year, table code, source type (DT/ST/DP)
# 4. Builds an inventory CSV
# 5. Creates a missing-file summary for expected years
#
# Output:
#   outputs/acs/inventory/acs_file_inventory.csv
#   outputs/acs/inventory/acs_folder_year_summary.csv
#   outputs/acs/inventory/acs_missing_expected_files.csv
# ============================================================

EXPECTED_YEARS = [2019, 2021, 2022, 2023]

# Update these only if your folder layout changes
CANDIDATE_RAW_DIRS = [
    "data/raw",
    "raw",
    "data/raw/acs",
    "data/acs"
]

# File name examples:
#   ACSDT5Y2023.B19013-Data.csv
#   ACSST5Y2022.S1101-Column-Metadata.csv
#   ACSDP5Y2021.DP04-Table-Notes.txt
FILE_PATTERN = re.compile(
    r"^ACS(?P<source_type>DT|ST|DP)5Y(?P<year>\d{4})\.(?P<table_code>[A-Z0-9]+)-(?P<file_kind>Data|Column-Metadata|Table-Notes)\.(?P<ext>csv|txt)$",
    re.IGNORECASE,
)

# Folder name examples:
#   MedianHouseholdIncome(B19013)_2023_2019
#   Poverty(S1701)_2023-2019
FOLDER_PATTERN = re.compile(
    r"^(?P<subject>.+?)\((?P<table_code>[A-Z0-9]+)\)_(?P<year_span>.+)$",
    re.IGNORECASE,
)


def find_raw_root(project_root: Path) -> Path:
    """Find the first existing ACS raw directory from candidate paths."""
    for rel_path in CANDIDATE_RAW_DIRS:
        candidate = project_root / rel_path
        if candidate.exists() and candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "Could not find ACS raw directory. Checked: "
        + ", ".join(str(project_root / p) for p in CANDIDATE_RAW_DIRS)
    )


def parse_folder_name(folder_name: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract subject, table_code, year_span from folder name if possible."""
    match = FOLDER_PATTERN.match(folder_name)
    if not match:
        return None, None, None
    return (
        match.group("subject"),
        match.group("table_code").upper(),
        match.group("year_span"),
    )


def parse_file_name(file_name: str) -> Optional[dict]:
    """Extract source_type, year, table_code, file_kind from file name."""
    match = FILE_PATTERN.match(file_name)
    if not match:
        return None
    return {
        "source_type": match.group("source_type").upper(),
        "year": int(match.group("year")),
        "table_code": match.group("table_code").upper(),
        "file_kind": match.group("file_kind"),
        "extension": match.group("ext").lower(),
    }


def infer_table_family(table_code: str) -> str:
    """Classify table family from table code."""
    code = table_code.upper()
    if code.startswith("DP"):
        return "Data Profile"
    if code.startswith("S"):
        return "Subject Table"
    if code.startswith("B") or code.startswith("C"):
        return "Detailed Table"
    return "Unknown"


def collect_inventory(raw_root: Path) -> pd.DataFrame:
    """
    Build row-level inventory.
    One row = one file found inside one ACS table folder.
    """
    rows: list[dict] = []

    for folder in sorted(raw_root.iterdir()):
        if not folder.is_dir():
            continue

        subject_from_folder, table_code_from_folder, year_span = parse_folder_name(folder.name)

        for file_path in sorted(folder.iterdir()):
            if not file_path.is_file():
                continue

            parsed = parse_file_name(file_path.name)
            if not parsed:
                # Ignore unrelated files silently
                continue

            table_code = parsed["table_code"]
            rows.append(
                {
                    "folder_name": folder.name,
                    "folder_path": str(folder),
                    "subject_from_folder": subject_from_folder,
                    "table_code_from_folder": table_code_from_folder,
                    "year_span_from_folder": year_span,
                    "file_name": file_path.name,
                    "file_path": str(file_path),
                    "source_type": parsed["source_type"],  # DT / ST / DP
                    "table_family": infer_table_family(table_code),
                    "table_code": table_code,
                    "year": parsed["year"],
                    "file_kind": parsed["file_kind"],  # Data / Column-Metadata / Table-Notes
                    "extension": parsed["extension"],
                    "is_expected_year": parsed["year"] in EXPECTED_YEARS,
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(
            f"No ACS files matched the expected naming pattern inside: {raw_root}"
        )

    return df.sort_values(
        ["table_family", "table_code", "year", "file_kind", "file_name"]
    ).reset_index(drop=True)


def build_year_summary(inventory_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build one row per folder + year, showing whether all 3 expected files exist.
    """
    summary = (
        inventory_df.pivot_table(
            index=[
                "folder_name",
                "subject_from_folder",
                "table_code",
                "table_family",
                "source_type",
                "year",
            ],
            columns="file_kind",
            values="file_name",
            aggfunc="count",
            fill_value=0,
        )
        .reset_index()
    )

    for col in ["Data", "Column-Metadata", "Table-Notes"]:
        if col not in summary.columns:
            summary[col] = 0

    summary["has_data_csv"] = summary["Data"] > 0
    summary["has_column_metadata_csv"] = summary["Column-Metadata"] > 0
    summary["has_table_notes_txt"] = summary["Table-Notes"] > 0
    summary["has_all_3_expected_files"] = (
        summary["has_data_csv"]
        & summary["has_column_metadata_csv"]
        & summary["has_table_notes_txt"]
    )

    return summary.sort_values(["table_code", "year"]).reset_index(drop=True)


def build_missing_expected(summary_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each folder/table, show missing years and missing file components
    against EXPECTED_YEARS = [2019, 2021, 2022, 2023]
    """
    rows: list[dict] = []

    folder_keys = (
        summary_df[
            ["folder_name", "subject_from_folder", "table_code", "table_family", "source_type"]
        ]
        .drop_duplicates()
        .to_dict(orient="records")
    )

    for key in folder_keys:
        sub = summary_df[
            (summary_df["folder_name"] == key["folder_name"])
            & (summary_df["table_code"] == key["table_code"])
        ]

        observed_years = set(sub["year"].tolist())

        for year in EXPECTED_YEARS:
            year_row = sub[sub["year"] == year]

            if year not in observed_years:
                rows.append(
                    {
                        **key,
                        "year": year,
                        "status": "missing_year_entirely",
                        "missing_data_csv": True,
                        "missing_column_metadata_csv": True,
                        "missing_table_notes_txt": True,
                    }
                )
                continue

            record = year_row.iloc[0]
            missing_data = not bool(record["has_data_csv"])
            missing_meta = not bool(record["has_column_metadata_csv"])
            missing_notes = not bool(record["has_table_notes_txt"])

            if missing_data or missing_meta or missing_notes:
                rows.append(
                    {
                        **key,
                        "year": year,
                        "status": "year_present_but_missing_components",
                        "missing_data_csv": missing_data,
                        "missing_column_metadata_csv": missing_meta,
                        "missing_table_notes_txt": missing_notes,
                    }
                )

    return pd.DataFrame(rows).sort_values(
        ["table_code", "year", "status"]
    ).reset_index(drop=True) if rows else pd.DataFrame(
        columns=[
            "folder_name",
            "subject_from_folder",
            "table_code",
            "table_family",
            "source_type",
            "year",
            "status",
            "missing_data_csv",
            "missing_column_metadata_csv",
            "missing_table_notes_txt",
        ]
    )


def print_console_summary(
    inventory_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    missing_df: pd.DataFrame,
    raw_root: Path,
    out_dir: Path,
) -> None:
    print("=" * 80)
    print("ACS INVENTORY COMPLETE")
    print("=" * 80)
    print(f"Raw root scanned : {raw_root}")
    print(f"Output folder    : {out_dir}")
    print()

    print("Files found by table code and year:")
    counts = (
        inventory_df.groupby(["table_code", "year"])["file_name"]
        .count()
        .reset_index(name="file_count")
        .sort_values(["table_code", "year"])
    )
    print(counts.to_string(index=False))
    print()

    print("Folders with all 3 expected files present:")
    ok = summary_df["has_all_3_expected_files"].sum()
    total = len(summary_df)
    print(f"{ok} / {total}")
    print()

    if missing_df.empty:
        print("No missing expected years or components found for the expected years.")
    else:
        print("Missing expected years / components:")
        print(missing_df.to_string(index=False))


def main() -> None:
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[3]

    raw_root = find_raw_root(project_root)
    out_dir = project_root / "outputs" / "acs" / "inventory"
    out_dir.mkdir(parents=True, exist_ok=True)

    inventory_df = collect_inventory(raw_root)
    summary_df = build_year_summary(inventory_df)
    missing_df = build_missing_expected(summary_df)

    inventory_file = out_dir / "acs_file_inventory.csv"
    summary_file = out_dir / "acs_folder_year_summary.csv"
    missing_file = out_dir / "acs_missing_expected_files.csv"

    inventory_df.to_csv(inventory_file, index=False)
    summary_df.to_csv(summary_file, index=False)
    missing_df.to_csv(missing_file, index=False)

    print_console_summary(inventory_df, summary_df, missing_df, raw_root, out_dir)


if __name__ == "__main__":
    main()