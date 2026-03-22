import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# 1. CONNECTION STRING

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 2. CHECK THE 4 COUNTIES FROM POSTGRES

lookup_counties = pd.read_sql("""
    select distinct countyfp
    from tract_puma1902_lookup
    order by countyfp
""", engine)

county_list = lookup_counties["countyfp"].tolist()
print("Counties found in tract_puma1902_lookup:", county_list)

# 3. FUNCTION TO PULL ACS5 TRACT DATA

def fetch_acs5_households(api_year, county_list):
    frames = []

    for county in county_list:
        url = (
            f"https://api.census.gov/data/{api_year}/acs/acs5"
            f"?get=NAME,B11001_001E"
            f"&for=tract:*"
            f"&in=state:17"
            f"&in=county:{county}"
        )

        print(f"Pulling {api_year} ACS5 for county {county} ...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data[1:], columns=data[0])

        df["api_year"] = api_year
        df["statefp"] = df["state"].str.zfill(2)
        df["countyfp"] = df["county"].str.zfill(3)
        df["tractce"] = df["tract"].str.zfill(6)
        df["tract_geoid"] = df["statefp"] + df["countyfp"] + df["tractce"]

        df["b11001_001e"] = pd.to_numeric(df["B11001_001E"], errors="coerce")

        df = df[[
            "api_year",
            "NAME",
            "statefp",
            "countyfp",
            "tractce",
            "tract_geoid",
            "b11001_001e"
        ]]

        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    return out

# 4. PULL 2022 AND 2023 ACS5

acs_2022 = fetch_acs5_households(2022, county_list)
acs_2023 = fetch_acs5_households(2023, county_list)

print("2022 raw ACS rows:", len(acs_2022))
print("2023 raw ACS rows:", len(acs_2023))

# 5. SAVE RAW TABLES TO POSTGRES

acs_2022.to_sql("acs5_2022_b11001_raw", engine, if_exists="replace", index=False)
acs_2023.to_sql("acs5_2023_b11001_raw", engine, if_exists="replace", index=False)

print("Saved tables:")
print(" - acs5_2022_b11001_raw")
print(" - acs5_2023_b11001_raw")
print("Done.")