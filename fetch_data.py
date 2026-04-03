"""
fetch_data.py

Data acquisition for the Climate & Environmental Justice project.
Fetches air quality data from OpenAQ API v3 and Census ACS data
for the Boston metro area at the Census tract level.

Prerequisites:
    - Register for an OpenAQ API key at https://explore.openaq.org/register
    - Register for a Census API key at https://api.census.gov/data/key_signup.html

Usage:
    1. Set your API keys below (or as environment variables)
    2. Run: python fetch_data.py
    3. Outputs: openaq_locations.csv, openaq_measurements.csv, census_tracts.csv
"""

import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv

# load .env file
load_dotenv()

# ── API Keys ──────────────────────────────────────────────────────────────────
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")

if not OPENAQ_API_KEY or not CENSUS_API_KEY:
    raise ValueError(
        "Missing API keys. Copy .env.example to .env and fill in your keys.\n"
        "  OPENAQ: https://explore.openaq.org/register\n"
        "  Census: https://api.census.gov/data/key_signup.html"
    )

# Boston Metro Bounding Box
# Covers Greater Boston: roughly Chelsea to Brookline to Quincy
BOSTON_BBOX = (-71.20, 42.23, -70.95, 42.42)  # (min_lon, min_lat, max_lon, max_lat)

# Boston-area county FIPS codes (Suffolk, Middlesex, Norfolk, Essex)
# State FIPS for Massachusetts = 25
MA_STATE_FIPS = "25"
BOSTON_COUNTY_FIPS = ["025", "017", "021", "009"]  # Suffolk, Middlesex, Norfolk, Essex


#  OPENAQ — Air Quality Monitoring Stations & Measurements

OPENAQ_BASE = "https://api.openaq.org/v3"
OPENAQ_HEADERS = {"X-API-Key": OPENAQ_API_KEY}


def fetch_openaq_locations():
    """
    Find all air quality monitoring stations in the Boston bounding box.
    Returns a DataFrame with location_id, name, lat, lon, and sensor info.
    """
    min_lon, min_lat, max_lon, max_lat = BOSTON_BBOX
    url = f"{OPENAQ_BASE}/locations"
    params = {
        "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}",
        "limit": 1000,
    }

    resp = requests.get(url, headers=OPENAQ_HEADERS, params=params)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for loc in data.get("results", []):
        coords = loc.get("coordinates", {})
        sensors = loc.get("sensors", [])
        for sensor in sensors:
            param = sensor.get("parameter", {})
            rows.append({
                "location_id": loc.get("id"),
                "location_name": loc.get("name"),
                "latitude": coords.get("latitude"),
                "longitude": coords.get("longitude"),
                "sensor_id": sensor.get("id"),
                "parameter": param.get("name"),
                "units": param.get("units"),
            })

    df = pd.DataFrame(rows)
    print(f"OpenAQ: Found {df['location_id'].nunique()} locations, "
          f"{len(df)} sensors in Boston area")
    return df


def fetch_openaq_daily_measurements(sensor_id, date_from="2024-01-01", date_to="2025-01-01"):
    """
    Fetch daily average measurements for a single sensor.
    Uses the /days endpoint for pre-aggregated daily means.
    """
    url = f"{OPENAQ_BASE}/sensors/{sensor_id}/days"
    all_results = []
    page = 1

    while True:
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": 1000,
            "page": page,
        }
        resp = requests.get(url, headers=OPENAQ_HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            break

        all_results.extend(results)
        page += 1
        time.sleep(0.2)  # respect rate limits

    return all_results


def fetch_all_measurements(locations_df, date_from="2024-01-01", date_to="2025-01-01",
                           parameters=None):
    """
    Fetch daily measurements for all sensors at all locations.
    Filters to specified parameters (default: PM2.5, O3, NO2).
    """
    if parameters is None:
        parameters = ["pm25", "o3", "no2"]

    # filter to sensors we care about
    target_sensors = locations_df[locations_df["parameter"].isin(parameters)].copy()
    print(f"Fetching daily data for {len(target_sensors)} sensors "
          f"({parameters}) from {date_from} to {date_to}...")

    all_rows = []
    for _, sensor_row in target_sensors.iterrows():
        sensor_id = sensor_row["sensor_id"]
        print(f"  Sensor {sensor_id} ({sensor_row['parameter']} "
              f"at {sensor_row['location_name']})...", end=" ")

        results = fetch_openaq_daily_measurements(sensor_id, date_from, date_to)

        for r in results:
            period = r.get("period", {})
            summary = r.get("summary", {})
            all_rows.append({
                "sensor_id": sensor_id,
                "location_id": sensor_row["location_id"],
                "location_name": sensor_row["location_name"],
                "latitude": sensor_row["latitude"],
                "longitude": sensor_row["longitude"],
                "parameter": sensor_row["parameter"],
                "units": sensor_row["units"],
                "date": period.get("datetimeFrom", {}).get("local", "")[:10],
                "avg_value": summary.get("avg"),
                "min_value": summary.get("min"),
                "max_value": summary.get("max"),
            })
        print(f"{len(results)} days")
        time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    print(f"\nOpenAQ: Collected {len(df)} daily measurement records")
    return df


#  CENSUS ACS — Socioeconomic & Transportation Data by Tract

CENSUS_BASE = "https://api.census.gov/data/2023/acs/acs5"

# Variable codes — see https://api.census.gov/data/2023/acs/acs5/variables.html
CENSUS_VARIABLES = {
    # Population
    "B01003_001E": "total_population",

    # Median household income
    "B19013_001E": "median_household_income",

    # Vehicles available (B25044)
    "B25044_001E": "total_housing_units",
    "B25044_003E": "owner_no_vehicle",
    "B25044_010E": "renter_no_vehicle",

    # Means of transportation to work (B08301)
    "B08301_001E": "total_commuters",
    "B08301_002E": "commute_car_truck_van",
    "B08301_010E": "commute_public_transit",
    "B08301_018E": "commute_bicycle",
    "B08301_019E": "commute_walked",
    "B08301_021E": "commute_work_from_home",

    # Race/ethnicity (B03002) — for environmental justice analysis
    "B03002_001E": "total_pop_race",
    "B03002_003E": "white_non_hispanic",
    "B03002_004E": "black_non_hispanic",
    "B03002_006E": "asian_non_hispanic",
    "B03002_012E": "hispanic_any_race",
}


def fetch_census_tracts():
    """
    Fetch ACS 5-year estimates at the Census tract level for
    Boston-area counties. Returns one DataFrame with all variables.
    """
    var_codes = list(CENSUS_VARIABLES.keys())
    # Census API allows up to 50 variables per request
    var_string = ",".join(["NAME"] + var_codes)

    all_tracts = []

    for county_fips in BOSTON_COUNTY_FIPS:
        url = CENSUS_BASE
        params = {
            "get": var_string,
            "for": "tract:*",
            "in": f"state:{MA_STATE_FIPS} county:{county_fips}",
            "key": CENSUS_API_KEY,
        }

        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        # first row is header
        header = data[0]
        rows = data[1:]
        df_county = pd.DataFrame(rows, columns=header)
        all_tracts.append(df_county)
        print(f"Census: {len(df_county)} tracts in county {county_fips}")

    df = pd.concat(all_tracts, ignore_index=True)

    # rename columns from codes to readable names
    df = df.rename(columns=CENSUS_VARIABLES)

    # build a GEOID for potential future spatial joins
    df["GEOID"] = df["state"] + df["county"] + df["tract"]

    # convert numeric columns
    numeric_cols = list(CENSUS_VARIABLES.values())
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # compute derived percentages
    df["pct_no_vehicle"] = (
        (df["owner_no_vehicle"].fillna(0) + df["renter_no_vehicle"].fillna(0))
        / df["total_housing_units"].replace(0, float("nan"))
        * 100
    ).round(2)

    df["pct_public_transit"] = (
        df["commute_public_transit"]
        / df["total_commuters"].replace(0, float("nan"))
        * 100
    ).round(2)

    df["pct_drive"] = (
        df["commute_car_truck_van"]
        / df["total_commuters"].replace(0, float("nan"))
        * 100
    ).round(2)

    df["pct_minority"] = (
        (df["total_pop_race"] - df["white_non_hispanic"])
        / df["total_pop_race"].replace(0, float("nan"))
        * 100
    ).round(2)

    print(f"\nCensus: {len(df)} total tracts across {len(BOSTON_COUNTY_FIPS)} counties")
    return df


#  MAIN — Fetch everything and save to CSV

def main():
    print("=" * 60)
    print("Fetching OpenAQ air quality data...")
    print("=" * 60)

    # Step 1: find monitoring stations in Boston
    locations = fetch_openaq_locations()
    locations.to_csv("openaq_locations.csv", index=False)
    print(f"Saved openaq_locations.csv\n")

    # Step 2: fetch daily measurements for PM2.5, O3, NO2
    measurements = fetch_all_measurements(
        locations,
        date_from="2024-01-01",
        date_to="2025-01-01",
        parameters=["pm25", "o3", "no2"]
    )
    measurements.to_csv("openaq_measurements.csv", index=False)
    print(f"Saved openaq_measurements.csv\n")

    print("=" * 60)
    print("Fetching Census ACS data...")
    print("=" * 60)

    # Step 3: fetch Census tract-level socioeconomic data
    tracts = fetch_census_tracts()
    tracts.to_csv("census_tracts.csv", index=False)
    print(f"Saved census_tracts.csv\n")

    # Summary
    print("=" * 60)
    print("DONE — Files saved:")
    print("  openaq_locations.csv     — monitoring station metadata")
    print("  openaq_measurements.csv  — daily air quality readings")
    print("  census_tracts.csv        — tract-level demographics & transportation")
    print("=" * 60)


if __name__ == "__main__":
    main()