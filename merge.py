"""
merge.py

Data merging and preprocessing layer.

Each function takes cleaned DataFrames from the cleaning layer
and returns merged DataFrames ready for analysis or visualization.

Functions:
    - aggregate_census_to_county(df) -> pd.DataFrame
    - aggregate_ghgrp_to_county(df) -> pd.DataFrame
    - aggregate_openaq_monthly(locations_df, measurements_df) -> pd.DataFrame
    - attach_county_to_openaq(df, filepath) -> pd.DataFrame | None
    - aggregate_openaq_to_county_month(df) -> pd.DataFrame
    - build_static_county_dataset(census_df, ghgrp_df) -> pd.DataFrame
    - build_monthly_county_dataset(static_df, openaq_df) -> pd.DataFrame
"""

from pathlib import Path
import warnings
import numpy as np
import pandas as pd

from clean import (
    clean_census,
    clean_openaq_locations,
    clean_openaq_measurements,
    clean_ghgrp,
)

#  File paths
RAW_CENSUS_PATH = "data/raw/census_tracts.csv"
RAW_OPENAQ_LOCATIONS_PATH = "data/raw/openaq_locations.csv"
RAW_OPENAQ_MEASUREMENTS_PATH = "data/raw/openaq_measurements.csv"
RAW_GHGRP_PATH = "data/raw/flight.xls"
OPENAQ_STATION_COUNTY_MAP_PATH = "data/raw/openaq_station_county_map.csv"

OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(exist_ok=True)

#  County reference - mapping from FIPS codes to county names for MA counties in the dataset
COUNTY_FIPS_TO_NAME = {
    "009": "Essex",
    "017": "Middlesex",
    "021": "Norfolk",
    "025": "Suffolk",
}

def standardize_county_name(series: pd.Series) -> pd.Series:
    """Standardize county names."""
    return (
        series.astype(str)
        .str.replace(" County", "", regex=False)
        .str.replace(" COUNTY", "", regex=False)
        .str.strip()
        .str.title()
    )

def safe_weighted_avg(values: pd.Series, weights: pd.Series) -> float:
    """Compute weighted average safely."""
    mask = values.notna() & weights.notna() & (weights > 0)
    if mask.sum() == 0:
        return np.nan
    return np.average(values[mask], weights=weights[mask])


#  Census ACS

def aggregate_census_to_county(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cleaned Census tract-level data to county level."""
    county_df = df.copy()
    county_df["county_fips"] = county_df["county"].astype(str).str.zfill(3)
    county_df["county_name"] = county_df["county_fips"].map(COUNTY_FIPS_TO_NAME)

    grouped_rows = []
    for county_fips, group in county_df.groupby("county_fips"):
        grouped_rows.append({
            "county_fips": county_fips,
            "county_name": group["county_name"].iloc[0],
            "num_tracts": len(group),
            "total_population": group["total_population"].sum(),
            "total_commuters": group["total_commuters"].sum(),
            "total_housing_units": group["total_housing_units"].sum(),
            "owner_no_vehicle": group["owner_no_vehicle"].sum(),
            "renter_no_vehicle": group["renter_no_vehicle"].sum(),
            "commute_car_truck_van": group["commute_car_truck_van"].sum(),
            "commute_public_transit": group["commute_public_transit"].sum(),
            "median_household_income_weighted": safe_weighted_avg(
                group["median_household_income"], group["total_population"]
            ),
            "pct_no_vehicle_weighted": safe_weighted_avg(
                group["pct_no_vehicle"], group["total_housing_units"]
            ),
            "pct_public_transit_weighted": safe_weighted_avg(
                group["pct_public_transit"], group["total_commuters"]
            ),
            "pct_drive_weighted": safe_weighted_avg(
                group["pct_drive"], group["total_commuters"]
            ),
            "pct_minority_weighted": safe_weighted_avg(
                group["pct_minority"], group["total_pop_race"]
            ),
        })
    county_df = pd.DataFrame(grouped_rows)
    county_df = county_df.sort_values("county_fips").reset_index(drop=True)
    return county_df


#  EPA GHGRP

def aggregate_ghgrp_to_county(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cleaned GHGRP facility-level data to county level."""
    county_df = df.copy()
    county_df["county_name"] = standardize_county_name(county_df["county"])
    county_df = (
        county_df.groupby("county_name", as_index=False)
        .agg(
            ghgrp_facility_count=("ghgrp_id", "nunique"),
            ghgrp_total_co2e=("ghg_quantity_co2e", "sum"),
            ghgrp_mean_facility_co2e=("ghg_quantity_co2e", "mean"),
            ghgrp_max_facility_co2e=("ghg_quantity_co2e", "max"),
        )
        .sort_values("county_name")
        .reset_index(drop=True)
    )
    return county_df


#  OpenAQ

def aggregate_openaq_monthly(
    locations_df: pd.DataFrame,
    measurements_df: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate cleaned OpenAQ data to station-month-parameter level."""
    merged = measurements_df.merge(
        locations_df[["location_id", "location_name", "latitude", "longitude", "parameter"]],
        on=["location_id", "location_name", "latitude", "longitude", "parameter"],
        how="left",
        validate="many_to_one",
    )
    monthly_df = (
        merged.groupby(
            ["location_id", "location_name", "latitude", "longitude",
             "parameter", "month", "month_name"],
            as_index=False,
        )
        .agg(
            monthly_avg_value=("avg_value", "mean"),
            monthly_min_value=("min_value", "min"),
            monthly_max_value=("max_value", "max"),
            days_observed=("date", "nunique"),
        )
        .sort_values(["location_id", "parameter", "month"])
        .reset_index(drop=True)
    )
    return monthly_df

def attach_county_to_openaq(
    df: pd.DataFrame,
    filepath: str = OPENAQ_STATION_COUNTY_MAP_PATH,
) -> pd.DataFrame | None:
    """Attach county names to OpenAQ stations using the mapping file."""
    path = Path(filepath)
    if not path.exists():
        warnings.warn(
            f"{filepath} not found. Skipping OpenAQ county merge."
        )
        return None
    mapping = pd.read_csv(filepath).copy()
    mapping["county_name"] = standardize_county_name(mapping["county"])
    merged = df.merge(
        mapping[["location_id", "county_name"]],
        on="location_id",
        how="left",
        validate="many_to_one",
    )
    merged = merged.dropna(subset=["county_name"]).reset_index(drop=True)
    return merged

def aggregate_openaq_to_county_month(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate OpenAQ data to county-month-parameter level."""
    county_df = (
        df.groupby(["county_name", "month", "month_name", "parameter"], as_index=False)
        .agg(
            air_quality_mean=("monthly_avg_value", "mean"),
            air_quality_min=("monthly_min_value", "min"),
            air_quality_max=("monthly_max_value", "max"),
            stations_reporting=("location_id", "nunique"),
            total_station_days=("days_observed", "sum"),
        )
        .sort_values(["county_name", "parameter", "month"])
        .reset_index(drop=True)
    )
    return county_df


#  Final merges

def build_static_county_dataset(
    census_df: pd.DataFrame,
    ghgrp_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge county-level Census and GHGRP data."""
    merged = census_df.merge(
        ghgrp_df,
        on="county_name",
        how="left",
        validate="one_to_one",
    )
    fill_cols = [
        "ghgrp_facility_count",
        "ghgrp_total_co2e",
        "ghgrp_mean_facility_co2e",
        "ghgrp_max_facility_co2e",
    ]
    merged[fill_cols] = merged[fill_cols].fillna(0)
    return merged


def build_monthly_county_dataset(
    static_df: pd.DataFrame,
    openaq_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge county-level static data with county-month OpenAQ data."""
    merged = openaq_df.merge(
        static_df,
        on="county_name",
        how="left",
        validate="many_to_one",
    )
    return merged

#  MAIN — Run all merging and save results

def main():
    """Run all merge steps and save processed data."""
    # read raw files
    census_raw = pd.read_csv(RAW_CENSUS_PATH)
    locations_raw = pd.read_csv(RAW_OPENAQ_LOCATIONS_PATH)
    measurements_raw = pd.read_csv(RAW_OPENAQ_MEASUREMENTS_PATH)

    # clean raw data
    census_clean = clean_census(census_raw)
    locations_clean = clean_openaq_locations(locations_raw)
    measurements_clean = clean_openaq_measurements(measurements_raw)
    ghgrp_clean = clean_ghgrp(RAW_GHGRP_PATH)

    # aggregate Census and GHGRP to county level
    census_county = aggregate_census_to_county(census_clean)
    ghgrp_county = aggregate_ghgrp_to_county(ghgrp_clean)
    merged_county_static = build_static_county_dataset(census_county, ghgrp_county)

    # save static outputs
    census_county.to_csv(OUTPUT_DIR / "census_county.csv", index=False)
    ghgrp_county.to_csv(OUTPUT_DIR / "ghgrp_county.csv", index=False)
    merged_county_static.to_csv(OUTPUT_DIR / "merged_county_static.csv", index=False)

    # aggregate OpenAQ to station-month level
    openaq_station_monthly = aggregate_openaq_monthly(locations_clean, measurements_clean)
    openaq_station_monthly.to_csv(OUTPUT_DIR / "openaq_station_monthly.csv", index=False)

    # attach county mapping
    openaq_with_county = attach_county_to_openaq(openaq_station_monthly)

    if openaq_with_county is not None and not openaq_with_county.empty:
        # aggregate OpenAQ to county-month level
        openaq_county_monthly = aggregate_openaq_to_county_month(openaq_with_county)

        # merge monthly OpenAQ with static county data
        merged_county_monthly = build_monthly_county_dataset(
            merged_county_static,
            openaq_county_monthly,
        )

        # save monthly outputs
        openaq_county_monthly.to_csv(OUTPUT_DIR / "openaq_county_monthly.csv", index=False)
        merged_county_monthly.to_csv(OUTPUT_DIR / "merged_county_monthly.csv", index=False)

    print("\n" + "=" * 60)
    print("Merging complete.")
    print("=" * 60)
    print(f"Saved outputs to: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()