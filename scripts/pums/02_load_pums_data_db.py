import io
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

data_dir = Path(r"D:\Projects\Community-Pulse\data\pums")

def clean_column_name(col):
    col = str(col).strip().lower()
    col = col.replace("%", "percent")
    col = col.replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "_", col)
    col = re.sub(r"_+", "_", col)
    col = col.strip("_")
    renames = {
        "st": "state",
        "type": "typehugq",
        "access": "accessinet",
        "ybl": "yrblt",
    }
    return renames.get(col, col)

for file_path in data_dir.glob("**/*.csv"):
    year = file_path.parent.name
    if "2020" in year:
        table_name = f"alice_{file_path.stem}_2020_exp_raw".lower()
    else:
        table_name = f"alice_{file_path.stem}_{year}_raw".lower()

    print(f"Loading {file_path} -> {table_name}")
    df = pd.read_csv(file_path)
    df.columns = [clean_column_name(c) for c in df.columns]

    
    df.head(0).to_sql(table_name, engine, if_exists="replace", index=False)

   
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False)
            buf.seek(0)
            cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", buf)
        raw_conn.commit()
    finally:
        raw_conn.close()

    print(f"Done: {table_name} ({len(df)} rows)")