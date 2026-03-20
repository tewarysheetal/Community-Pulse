import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import re

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

data_dir = Path(r"D:\Projects\Community-Pulse\data\combined-data")

def clean_column_name(col):
    col = str(col).strip().lower()
    col = col.replace("%", "percent")
    col = col.replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_")

def make_postgres_safe_columns(cols, max_len=63):
    seen = set()
    result = []
    for col in cols:
        candidate = col[:max_len]
        if candidate not in seen:
            seen.add(candidate)
            result.append(candidate)
        else:
            i = 1
            while True:
                suffix = f"_{i}"
                candidate = col[:max_len - len(suffix)] + suffix
                if candidate not in seen:
                    seen.add(candidate)
                    result.append(candidate)
                    break
                i += 1
    return result

for file_path in data_dir.glob("*_combined.csv"):
    table_name = file_path.stem.replace("_combined", "").lower()

    df = pd.read_csv(file_path, low_memory=False)

    cleaned_cols = [clean_column_name(col) for col in df.columns]
    final_cols = make_postgres_safe_columns(cleaned_cols, max_len=63)

    df.columns = final_cols

    print(f"\nLoading {file_path.name}")
    print(f"Columns: {len(df.columns)}")
    print(f"Unique after postgres-safe rename: {len(set(df.columns))}")

    dupes = df.columns[df.columns.duplicated()].tolist()
    if dupes:
        print("Still duplicated:", dupes[:10])
        continue

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

    print(f"Loaded {file_path.name} into table {table_name}")

print("Done")