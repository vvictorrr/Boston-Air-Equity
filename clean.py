"""
clean.py

Data cleaning and standardization layer.

Each function takes a raw DataFrame from the acquisition layer
and returns a cleaned DataFrame ready for merging.

Functions:
    - clean_census(df) -> pd.DataFrame
    - clean_openaq_locations(df) -> pd.DataFrame
    - clean_openaq_measurements(df) -> pd.DataFrame
    - clean_ghgrp(filepath) -> pd.DataFrame
"""

import pandas as pd
import numpy as np


#  Census ACS

def clean_census(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Census ACS tract-level data.

    Handles:
        - Replaces Census null sentinel (-666666666) with NaN
        - Drops uninhabited tracts (population == 0)
        - Ensures GEOID is a zero-padded string for spatial joins
        - Recalculates derived percentages to fill gaps
        - Drops tracts missing critical fields
    """
    cleaned = df.copy()

    # replace Census null sentinel with NaN
    cleaned = cleaned.replace(-666666666, np.nan)

    # drop uninhabited tracts (water bodies, parks, etc.)
    cleaned = cleaned[cleaned["total_population"] > 0].copy()

    # ensure GEOID is a zero-padded 11-character string
    # (state 2 + county 3 + tract 6)
    cleaned["GEOID"] = cleaned["GEOID"].astype(str).str.zfill(11)

    # recalculate derived percentages to fill any gaps
    cleaned["pct_no_vehicle"] = (
        (cleaned["owner_no_vehicle"].fillna(0) + cleaned["renter_no_vehicle"].fillna(0))
        / cleaned["total_housing_units"].replace(0, np.nan)
        * 100
    ).round(2)

    cleaned["pct_public_transit"] = (
        cleaned["commute_public_transit"]
        / cleaned["total_commuters"].replace(0, np.nan)
        * 100
    ).round(2)

    cleaned["pct_drive"] = (
        cleaned["commute_car_truck_van"]
        / cleaned["total_commuters"].replace(0, np.nan)
        * 100
    ).round(2)

    cleaned["pct_minority"] = (
        (cleaned["total_pop_race"] - cleaned["white_non_hispanic"])
        / cleaned["total_pop_race"].replace(0, np.nan)
        * 100
    ).round(2)

    # drop tracts still missing critical fields after recalculation
    cleaned = cleaned.dropna(subset=["median_household_income", "total_population"])

    cleaned = cleaned.reset_index(drop=True)
    print(f"Census: {len(cleaned)} tracts after cleaning "
          f"(dropped {len(df) - len(cleaned)} empty/invalid tracts)")
    return cleaned


#  OpenAQ — Locations

# parameters relevant to air quality analysis
RELEVANT_PARAMS = ["pm25", "o3", "no2"]

def clean_openaq_locations(df: pd.DataFrame) -> pd.DataFrame:
    """Clean OpenAQ location/sensor metadata.

    Handles:
        - Filters to relevant air quality parameters (pm25, o3, no2)
        - Drops duplicate location-parameter pairs (keeps first sensor)
        - Validates coordinates are within Boston bounding box
    """
    cleaned = df.copy()

    # filter to relevant parameters only
    cleaned = cleaned[cleaned["parameter"].isin(RELEVANT_PARAMS)].copy()

    # drop duplicate sensors for the same location-parameter pair
    # (keep the first sensor if a location has multiple for one parameter)
    cleaned = cleaned.drop_duplicates(
        subset=["location_id", "parameter"],
        keep="first"
    )

    # validate coordinates are roughly in Boston metro area
    cleaned = cleaned[
        (cleaned["latitude"].between(42.2, 42.5)) &
        (cleaned["longitude"].between(-71.2, -70.9))
    ].copy()

    cleaned = cleaned.reset_index(drop=True)
    print(f"OpenAQ locations: {cleaned['location_id'].nunique()} stations, "
          f"{len(cleaned)} sensors after cleaning")
    return cleaned



#  OpenAQ — Measurements

def clean_openaq_measurements(df: pd.DataFrame) -> pd.DataFrame:
    """Clean OpenAQ daily measurement data.

    Handles:
        - Filters to relevant parameters (pm25, o3, no2)
        - Removes negative values (instrument noise)
        - Parses date column to datetime, extracts month
        - Trims to the target year (2024)
        - Drops duplicate station-parameter-date entries
    """
    cleaned = df.copy()

    # filter to relevant parameters
    cleaned = cleaned[cleaned["parameter"].isin(RELEVANT_PARAMS)].copy()

    # remove negative avg_value readings (instrument noise / calibration)
    cleaned = cleaned[cleaned["avg_value"] >= 0].copy()

    # parse dates
    cleaned["date"] = pd.to_datetime(cleaned["date"], errors="coerce")
    cleaned = cleaned.dropna(subset=["date"])

    # trim to 2024 only
    cleaned = cleaned[cleaned["date"].dt.year == 2024].copy()

    # extract month for animated visualization
    cleaned["month"] = cleaned["date"].dt.month
    cleaned["month_name"] = cleaned["date"].dt.strftime("%B")

    # drop duplicates (same station, parameter, date)
    cleaned = cleaned.drop_duplicates(
        subset=["location_id", "parameter", "date"],
        keep="first"
    )

    # sort chronologically
    cleaned = cleaned.sort_values(["location_id", "parameter", "date"])
    cleaned = cleaned.reset_index(drop=True)

    print(f"OpenAQ measurements: {len(cleaned)} daily records after cleaning "
          f"({cleaned['location_id'].nunique()} stations, "
          f"{cleaned['date'].min().date()} to {cleaned['date'].max().date()})")
    return cleaned



#  EPA GHGRP (FLIGHT export)

def clean_ghgrp(filepath: str) -> pd.DataFrame:
    """Clean EPA GHGRP FLIGHT export data.

    Handles:
        - Reads xls/xlsx with correct header row (skips metadata rows)
        - Renames columns to clean snake_case names
        - Converts emissions and coordinates to numeric
        - Drops facilities missing coordinates or emissions
        - Filters to Massachusetts only (safety check)
    """
    # read with header at row 4 (0-indexed) to skip metadata rows
    if filepath.endswith((".xls", ".xlsx")):
        raw = pd.read_excel(filepath, header=5)
    else:
        raw = pd.read_csv(filepath, header=5)

    # rename columns to clean names
    column_map = {
        "REPORTING YEAR": "year",
        "FACILITY NAME": "facility_name",
        "GHGRP ID": "ghgrp_id",
        "REPORTED ADDRESS": "address",
        "LATITUDE": "latitude",
        "LONGITUDE": "longitude",
        "CITY NAME": "city",
        "COUNTY NAME": "county",
        "STATE": "state",
        "ZIP CODE": "zip_code",
        "PARENT COMPANIES": "parent_company",
        "GHG QUANTITY (METRIC TONS CO2e)": "ghg_quantity_co2e",
        "SUBPARTS": "subparts",
    }
    cleaned = raw.rename(columns=column_map)

    # keep only mapped columns that exist
    valid_cols = [c for c in column_map.values() if c in cleaned.columns]
    cleaned = cleaned[valid_cols].copy()

    # convert numeric fields
    cleaned["latitude"] = pd.to_numeric(cleaned["latitude"], errors="coerce")
    cleaned["longitude"] = pd.to_numeric(cleaned["longitude"], errors="coerce")
    cleaned["ghg_quantity_co2e"] = pd.to_numeric(cleaned["ghg_quantity_co2e"], errors="coerce")

    # drop rows missing coordinates or emissions
    cleaned = cleaned.dropna(subset=["latitude", "longitude", "ghg_quantity_co2e"])

    # filter to Massachusetts only
    cleaned = cleaned[cleaned["state"] == "MA"].copy()

    # sort by emissions descending
    cleaned = cleaned.sort_values("ghg_quantity_co2e", ascending=False)
    cleaned = cleaned.reset_index(drop=True)

    print(f"GHGRP: {len(cleaned)} facilities in MA after cleaning "
          f"(total emissions: {cleaned['ghg_quantity_co2e'].sum():,.0f} metric tons CO2e)")
    return cleaned


#  MAIN — Run all cleaning and save results

def main():
    """Run all cleaning steps and save cleaned data."""
    print("=" * 60)
    print("Cleaning all data sources...")
    print("=" * 60)

    # Census
    print("\n--- Census ACS ---")
    census_raw = pd.read_csv("census_tracts.csv")
    census_clean = clean_census(census_raw)
    census_clean.to_csv("census_tracts_clean.csv", index=False)

    # OpenAQ locations
    print("\n--- OpenAQ Locations ---")
    locations_raw = pd.read_csv("openaq_locations.csv")
    locations_clean = clean_openaq_locations(locations_raw)
    locations_clean.to_csv("openaq_locations_clean.csv", index=False)

    # OpenAQ measurements
    print("\n--- OpenAQ Measurements ---")
    measurements_raw = pd.read_csv("openaq_measurements.csv")
    measurements_clean = clean_openaq_measurements(measurements_raw)
    measurements_clean.to_csv("openaq_measurements_clean.csv", index=False)

    # GHGRP
    print("\n--- EPA GHGRP ---")
    ghgrp_clean = clean_ghgrp("flight.xls")
    ghgrp_clean.to_csv("ghgrp_clean.csv", index=False)

    print("\n" + "=" * 60)
    print("DONE - Cleaned files saved:")
    print("  census_tracts_clean.csv")
    print("  openaq_locations_clean.csv")
    print("  openaq_measurements_clean.csv")
    print("  ghgrp_clean.csv")
    print("=" * 60)


if __name__ == "__main__":
    main()