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

data_dir = Path(r"D:\Projects\Campus-Community-Compact-Project\data\combined-data")

def clean_column_name(col):
    col = col.strip().lower()
    col = col.replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_")

for file_path in data_dir.glob("*_combined.csv"):
    table_name = file_path.stem.replace("_combined", "").lower()

    df = pd.read_csv(file_path, low_memory=False)
    df.columns = [clean_column_name(col) for col in df.columns]

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False
    )

    print(f"Loaded {file_path.name} into table {table_name}")

print("Done")