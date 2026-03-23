from pathlib import Path
from typing import List, Tuple
import textwrap
import os

from dotenv import load_dotenv

try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None

try:
    import pandas as pd
except ImportError:
    pd = None

OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\sql\pums")
DATA_OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\outputs\pums\tableau-data")
YEARS = [2019, 2021, 2022, 2023]
RUN_SQL = True
EXPORT_CSV = True

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


HH_COMP_SORT_SQL = """
case coalesce(hh_comp_key, 'other')
    when '1_adult_0_child' then 1
    when '1_adult_1_child' then 2
    when '1_adult_2plus_child' then 3
    when '2_adult_0_child' then 4
    when '2_adult_1_child' then 5
    when '2_adult_2plus_child' then 6
    when '3plus_adult_0_child' then 7
    when '3plus_adult_1plus_child' then 8
    else 9
end
""".strip()


def sql_create_below_alice_dataset(year: int) -> str:
    return textwrap.dedent(
        f"""
        drop table if exists alice_below_alice_households_{year};

        create table alice_below_alice_households_{year} as
        select
            {year}::integer as year,
            t.*
        from alice_household_final_{year} t
        where below_alice_flag = 1;
        """
    ).strip() + "\n"



def sql_create_profile_table(year: int, source_table: str, profile_table: str, population_label: str) -> str:
    return textwrap.dedent(
        f"""
        drop table if exists {profile_table};

        create table {profile_table} as
        with base as (
            select *
            from {source_table}
        ),
        totals as (
            select
                count(*)::numeric as unweighted_records,
                coalesce(sum(analysis_weight::numeric), 0)::numeric as weighted_households
            from base
        )
        select
            {year}::integer as year,
            '{population_label}'::text as population_type,
            'overall'::text as metric_group,
            'weighted_households'::text as metric_name,
            round(t.weighted_households, 4)::numeric as metric_value,
            100.0000::numeric as pct_of_households,
            10::integer as sort_order
        from totals t

        union all

        select
            {year},
            '{population_label}',
            'overall',
            'unweighted_records',
            t.unweighted_records,
            null::numeric,
            20
        from totals t

        union all

        select
            {year},
            '{population_label}',
            'overall',
            'weighted_avg_household_size',
            round(sum(analysis_weight::numeric * coalesce(person_count::numeric, np::numeric, 0)) / nullif(sum(analysis_weight::numeric), 0), 4),
            null::numeric,
            30
        from base

        union all

        select
            {year},
            '{population_label}',
            'overall',
            'weighted_avg_adult_count',
            round(sum(analysis_weight::numeric * coalesce(adult_count::numeric, 0)) / nullif(sum(analysis_weight::numeric), 0), 4),
            null::numeric,
            40
        from base

        union all

        select
            {year},
            '{population_label}',
            'overall',
            'weighted_avg_child_count',
            round(sum(analysis_weight::numeric * coalesce(noc::numeric, 0)) / nullif(sum(analysis_weight::numeric), 0), 4),
            null::numeric,
            50
        from base

        union all

        select
            {year},
            '{population_label}',
            'income',
            'weighted_avg_real_income',
            round(sum(analysis_weight::numeric * coalesce(hincp_adj_real, 0)) / nullif(sum(analysis_weight::numeric), 0), 2),
            null::numeric,
            60
        from base

        union all

        select
            {year},
            '{population_label}',
            'income',
            'median_real_income_unweighted',
            round((percentile_cont(0.5) within group (order by hincp_adj_real))::numeric, 2),
            null::numeric,
            70
        from base
        where hincp_adj_real is not null

        union all

        select
            {year},
            '{population_label}',
            'income',
            'weighted_avg_annual_alice_threshold',
            round(sum(analysis_weight::numeric * coalesce(annual_alice_threshold, 0)) / nullif(sum(analysis_weight::numeric), 0), 2),
            null::numeric,
            80
        from base

        union all

        select
            {year},
            '{population_label}',
            'student_labor',
            'share_student_heavy',
            round(sum(case when coalesce(student_heavy_flag, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(student_heavy_flag, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            90
        from base

        union all

        select
            {year},
            '{population_label}',
            'student_labor',
            'share_has_any_student',
            round(sum(case when coalesce(has_any_student, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(has_any_student, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            100
        from base

        union all

        select
            {year},
            '{population_label}',
            'student_labor',
            'share_has_any_likely_college_student',
            round(sum(case when coalesce(has_any_likely_college_student, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(has_any_likely_college_student, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            110
        from base

        union all

        select
            {year},
            '{population_label}',
            'student_labor',
            'share_reference_person_student',
            round(sum(case when coalesce(reference_person_student_flag, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(reference_person_student_flag, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            120
        from base

        union all

        select
            {year},
            '{population_label}',
            'student_labor',
            'share_has_any_employed_person',
            round(sum(case when coalesce(has_any_employed_person, 0) = 1 then analysis_weight::numeric else 0 end), 4),
            round(100.0 * sum(case when coalesce(has_any_employed_person, 0) = 1 then analysis_weight::numeric else 0 end) / nullif(sum(analysis_weight::numeric), 0), 4),
            130
        from base

        union all

        select
            {year},
            '{population_label}',
            'composition_hh_comp_key',
            coalesce(hh_comp_key, 'other') as metric_name,
            round(sum(analysis_weight::numeric), 4) as metric_value,
            round(100.0 * sum(analysis_weight::numeric) / nullif(max(t.weighted_households), 0), 4) as pct_of_households,
            200 + {HH_COMP_SORT_SQL} as sort_order
        from base b
        cross join totals t
        group by coalesce(hh_comp_key, 'other')
        order by sort_order, metric_name;
        """
    ).strip() + "\n"



def write_file(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")



def validate_db_settings() -> None:
    missing = [
        key for key, value in {
            "DB_HOST": DB_HOST,
            "DB_PORT": DB_PORT,
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError("Missing DB environment variables: " + ", ".join(missing))



def get_engine():
    if create_engine is None:
        raise ImportError(
            "sqlalchemy is required. Install with: pip install sqlalchemy psycopg2-binary"
        )
    validate_db_settings()
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )



def execute_sql_files(files: List[Path]) -> None:
    if not RUN_SQL:
        return

    engine = get_engine()
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for file_path in files:
            sql_text = file_path.read_text(encoding="utf-8")
            print(f"Executing: {file_path}")
            cursor.execute(sql_text)
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()
        engine.dispose()



def export_tables_to_csv(exports: List[Tuple[int, str]]) -> None:
    if not EXPORT_CSV:
        return
    if pd is None:
        raise ImportError("pandas is required when EXPORT_CSV = True. Install with: pip install pandas")

    engine = get_engine()
    try:
        combined_frames = {
            "below_alice_households": [],
            "below_alice_stat_profile": [],
            "nonstudent_stat_profile": [],
        }

        for year, table_name in exports:
            year_dir = DATA_OUTPUT_DIR / str(year)
            year_dir.mkdir(parents=True, exist_ok=True)

            if "stat_profile" in table_name:
                query = f"select * from {table_name} order by sort_order, metric_group, metric_name"
            else:
                query = f"select * from {table_name}"

            print(f"Exporting CSV: {table_name}")
            df = pd.read_sql(query, engine)
            csv_path = year_dir / f"{table_name}.csv"
            df.to_csv(csv_path, index=False)

            if table_name.startswith("alice_below_alice_households_"):
                combined_frames["below_alice_households"].append(df)
            elif table_name.startswith("alice_below_alice_stat_profile_"):
                combined_frames["below_alice_stat_profile"].append(df)
            elif table_name.startswith("alice_nonstudent_stat_profile_"):
                combined_frames["nonstudent_stat_profile"].append(df)

        for name, frames in combined_frames.items():
            if frames:
                combined_df = pd.concat(frames, ignore_index=True)
                combined_df.to_csv(DATA_OUTPUT_DIR / f"{name}_all_years.csv", index=False)
    finally:
        engine.dispose()



def main() -> None:
    files: List[Path] = []
    exports: List[Tuple[int, str]] = []

    for year in YEARS:
        year_dir = OUTPUT_DIR / str(year)

        below_dataset_table = f"alice_below_alice_households_{year}"
        below_profile_table = f"alice_below_alice_stat_profile_{year}"
        nonstudent_profile_table = f"alice_nonstudent_stat_profile_{year}"

        below_dataset_file = year_dir / f"12a_create_pums_below_alice_households_{year}.sql"
        below_profile_file = year_dir / f"12b_create_pums_below_alice_stat_profile_{year}.sql"
        nonstudent_profile_file = year_dir / f"12c_create_pums_nonstudent_stat_profile_{year}.sql"

        write_file(below_dataset_file, sql_create_below_alice_dataset(year))
        write_file(
            below_profile_file,
            sql_create_profile_table(
                year=year,
                source_table=below_dataset_table,
                profile_table=below_profile_table,
                population_label="complete_below_alice",
            ),
        )
        write_file(
            nonstudent_profile_file,
            sql_create_profile_table(
                year=year,
                source_table=f"alice_nonstudent_households_{year}",
                profile_table=nonstudent_profile_table,
                population_label="nonstudent_below_alice",
            ),
        )

        files.extend([below_dataset_file, below_profile_file, nonstudent_profile_file])
        exports.extend([
            (year, below_dataset_table),
            (year, below_profile_table),
            (year, nonstudent_profile_table),
        ])

    print(f"Generated SQL files under: {OUTPUT_DIR}")
    execute_sql_files(files)
    export_tables_to_csv(exports)
    print(f"Done. Below-ALICE datasets and profile tables created. CSV exports saved under: {DATA_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
