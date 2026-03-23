import re
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

SQL_FILE = Path(r"D:\Projects\Community-Pulse\sql\pums\Final_Validation_Check.sql")
OUTPUT_DIR = Path(r"D:\Projects\Community-Pulse\outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

sql_text = SQL_FILE.read_text(encoding="utf-8")

# Split into query blocks by stripping /* ... */ comment headers
blocks = re.split(r"/\*.*?\*/", sql_text, flags=re.DOTALL)
queries = [b.strip().rstrip(";").strip() for b in blocks if b.strip()]

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
all_frames = []
passed = 0
failed = 0

print(f"Running {len(queries)} validation queries...\n")

for i, sql in enumerate(queries, 1):
    try:
        df = pd.read_sql(sql, engine)

        # Name the file after the validation_name column if present
        if "validation_name" in df.columns:
            name = df["validation_name"].iloc[0]
        else:
            name = f"query_{i:02d}"

        out_path = OUTPUT_DIR / f"{name}.csv"
        df.to_csv(out_path, index=False)
        print(f"  [{i}] {name} — {len(df)} row(s) -> {out_path.name}")

        all_frames.append(df)
        passed += 1

    except Exception as e:
        print(f"  [{i}] ERROR: {e}")
        failed += 1

# Save a combined output with all results that share compatible schemas
if all_frames:
    combined_path = OUTPUT_DIR / f"final_validation_combined_{timestamp}.csv"
    try:
        combined = pd.concat(all_frames, ignore_index=True)
        combined.to_csv(combined_path, index=False)
        print(f"\nCombined output saved: {combined_path.name}")
    except Exception:
        # Queries have different schemas — save each frame individually (already done above)
        print("\nNote: combined output skipped (queries have different column schemas).")

print(f"\nDone. {passed} passed, {failed} failed.")
print(f"Output folder: {OUTPUT_DIR}")
