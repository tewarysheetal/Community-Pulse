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
file_path= data_dir / "Tract-Illinois.csv"

df=pd.read_csv(file_path,dtype=str)

df.columns = [c.strip().lower() for c in df.columns]
df = df[["statefp", "countyfp", "tractce", "puma5ce", "champgf"]].copy()
df["statefp"] = df["statefp"].str.zfill(2)
df["countyfp"] = df["countyfp"].str.zfill(3)
df["tractce"] = df["tractce"].str.zfill(6)
df["puma5ce"] = df["puma5ce"].str.zfill(5)
df["geoid"] = df["statefp"] + df["countyfp"] + df["tractce"]

df.to_sql("tract_il", engine, if_exists="replace", index=False)

print(f"Done: tract_il ({len(df)} rows)")

