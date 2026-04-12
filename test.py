"""

Covers:
    - Data loading/fetching functions (fetch_data.py)
    - Data cleaning functions (clean.py)
    - Merge logic (merge.py)
    - Data validation (ranges, types, structure)

"""

import warnings
import numpy as np
import pandas as pd
import pytest

from clean import (
    clean_census,
    clean_openaq_locations,
    clean_openaq_measurements,
    RELEVANT_PARAMS,
)
from merge import (
    aggregate_census_to_county,
    aggregate_ghgrp_to_county,
    aggregate_openaq_monthly,
    aggregate_openaq_to_county_month,
    build_static_county_dataset,
    build_monthly_county_dataset,
    standardize_county_name,
    safe_weighted_avg,
    COUNTY_FIPS_TO_NAME,
)

@pytest.fixture
def raw_census_df():
    """Minimal raw Census ACS DataFrame mimicking real API output."""
    return pd.DataFrame({
        "GEOID":                  ["2502500101", "2501700201", "2502500301", "2502500401"],
        "state":                  ["25", "25", "25", "25"],
        "county":                 ["025", "017", "025", "025"],
        "tract":                  ["000101", "000201", "000301", "000401"],
        "total_population":       [3000, 1500, 0, 2000],        # tract 3 is uninhabited
        "median_household_income":[ 65000, 80000, -666666666, 72000],  # tract 3 has sentinel
        "total_housing_units":    [1200, 600, 0, 900],
        "owner_no_vehicle":       [100, 50, 0, 80],
        "renter_no_vehicle":      [200, 100, 0, 150],
        "total_commuters":        [1500, 750, 0, 1000],
        "commute_car_truck_van":  [900, 500, 0, 600],
        "commute_public_transit": [400, 150, 0, 250],
        "commute_bicycle":        [50, 20, 0, 30],
        "commute_walked":         [80, 40, 0, 60],
        "commute_work_from_home": [70, 40, 0, 60],
        "total_pop_race":         [3000, 1500, 0, 2000],
        "white_non_hispanic":     [1800, 1200, 0, 1000],
        "black_non_hispanic":     [500, 100, 0, 400],
        "asian_non_hispanic":     [400, 150, 0, 400],
        "hispanic_any_race":      [300, 50, 0, 200],
    })


@pytest.fixture
def clean_census_df(raw_census_df):
    return clean_census(raw_census_df)


@pytest.fixture
def raw_locations_df():
    """Minimal raw OpenAQ locations DataFrame."""
    return pd.DataFrame({
        "location_id":   [1, 1, 2, 2, 3, 4],
        "location_name": ["Station A", "Station A", "Station B", "Station B",
                          "Station C", "Station D"],
        "latitude":      [42.36, 42.36, 42.45, 42.45, 99.0, 42.30],
        "longitude":     [-71.06, -71.06, -71.10, -71.10, -71.06, -71.08],
        "sensor_id":     [10, 11, 20, 21, 30, 40],
        "parameter":     ["pm25", "pm25", "o3", "no2", "pm25", "co"],   # co is irrelevant
        "units":         ["µg/m³", "µg/m³", "ppm", "ppb", "µg/m³", "ppm"],
    })


@pytest.fixture
def clean_locations_df(raw_locations_df):
    return clean_openaq_locations(raw_locations_df)


@pytest.fixture
def raw_measurements_df():
    """Minimal raw OpenAQ measurements DataFrame."""
    return pd.DataFrame({
        "location_id":   [1, 1, 1, 2, 2, 2, 1],
        "location_name": ["Station A"] * 3 + ["Station B"] * 3 + ["Station A"],
        "latitude":      [42.36] * 6 + [42.36],
        "longitude":     [-71.06] * 6 + [-71.06],
        "sensor_id":     [10, 10, 10, 20, 20, 20, 10],
        "parameter":     ["pm25"] * 3 + ["o3"] * 3 + ["pm25"],
        "units":         ["µg/m³"] * 3 + ["ppm"] * 3 + ["µg/m³"],
        "date":          ["2024-01-15", "2024-02-10", "2024-02-10",   # dup on row 2&3
                          "2024-03-05", "2024-06-20", "2023-11-01",   # 2023 = out of range
                          "2024-01-15"],                               # exact dup of row 0
        "avg_value":     [12.5, 8.0, 8.0, 45.0, 55.0, 40.0, 12.5],
        "min_value":     [8.0, 5.0, 5.0, 30.0, 40.0, 25.0, 8.0],
        "max_value":     [18.0, 12.0, 12.0, 60.0, 70.0, 55.0, 18.0],
    })


@pytest.fixture
def clean_measurements_df(raw_measurements_df):
    return clean_openaq_measurements(raw_measurements_df)


@pytest.fixture
def census_county_df(clean_census_df):
    return aggregate_census_to_county(clean_census_df)


@pytest.fixture
def ghgrp_county_df():
    """Minimal county-level GHGRP DataFrame (post-aggregation shape)."""
    return pd.DataFrame({
        "county_name":             ["Middlesex", "Suffolk"],
        "ghgrp_facility_count":    [3, 5],
        "ghgrp_total_co2e":        [150000.0, 800000.0],
        "ghgrp_mean_facility_co2e":[50000.0, 160000.0],
        "ghgrp_max_facility_co2e": [90000.0, 350000.0],
    })


# 1. Data Loading

class TestFetchDataHelpers:

    def test_census_variables_are_valid_mapping(self):
        """fetch_data.py CENSUS_VARIABLES maps ACS codes to readable names."""
        from fetch_data import CENSUS_VARIABLES
        assert isinstance(CENSUS_VARIABLES, dict)
        assert len(CENSUS_VARIABLES) >= 10, "Expected at least 10 Census variables"
        # all keys should look like ACS variable codes (e.g., B01003_001E)
        for code in CENSUS_VARIABLES:
            assert code.startswith("B"), f"Unexpected variable code: {code}"
            assert code.endswith("E"), f"Code should end with 'E' (estimate): {code}"

    def test_boston_bbox_is_valid(self):
        """BOSTON_BBOX should define a valid geographic bounding box."""
        from fetch_data import BOSTON_BBOX
        min_lon, min_lat, max_lon, max_lat = BOSTON_BBOX
        assert min_lon < max_lon, "min_lon must be less than max_lon"
        assert min_lat < max_lat, "min_lat must be less than max_lat"
        # Boston is roughly 42.2–42.75 N, -71.25 to -70.70 W
        assert -72 < min_lon < -70, "Longitude outside expected Boston range"
        assert 42 < min_lat < 43,   "Latitude outside expected Boston range"

    def test_boston_county_fips_are_strings(self):
        """County FIPS codes should be zero-padded 3-character strings."""
        from fetch_data import BOSTON_COUNTY_FIPS
        for fips in BOSTON_COUNTY_FIPS:
            assert isinstance(fips, str), f"FIPS {fips!r} is not a string"
            assert len(fips) == 3,        f"FIPS {fips!r} is not 3 characters"
            assert fips.isdigit(),        f"FIPS {fips!r} contains non-digits"


# 2. Data Cleaning (Census)

class TestCleanCensus:

    def test_uninhabited_tracts_are_dropped(self, raw_census_df, clean_census_df):
        """Tracts with total_population == 0 must be removed."""
        raw_zeros = (raw_census_df["total_population"] == 0).sum()
        assert raw_zeros > 0, "Fixture needs at least one uninhabited tract"
        assert (clean_census_df["total_population"] == 0).sum() == 0

    def test_census_null_sentinel_replaced(self, clean_census_df):
        """Census sentinel value -666666666 must be replaced with NaN."""
        for col in clean_census_df.select_dtypes(include="number").columns:
            assert (clean_census_df[col] == -666666666).sum() == 0, \
                f"Sentinel found in column: {col}"

    def test_geoid_is_zero_padded_11_chars(self, clean_census_df):
        """GEOID must be exactly 11 characters, zero-padded."""
        assert clean_census_df["GEOID"].str.len().eq(11).all(), \
            "Some GEOIDs are not 11 characters long"

    def test_derived_percentages_are_in_range(self, clean_census_df):
        """Derived percentage columns must be between 0 and 100."""
        pct_cols = ["pct_no_vehicle", "pct_public_transit", "pct_drive", "pct_minority"]
        for col in pct_cols:
            if col in clean_census_df.columns:
                valid = clean_census_df[col].dropna()
                assert (valid >= 0).all() and (valid <= 100).all(), \
                    f"{col} has values outside [0, 100]"

    def test_rows_with_missing_critical_fields_dropped(self, clean_census_df):
        """Rows missing median_household_income or total_population must be dropped."""
        assert clean_census_df["median_household_income"].isna().sum() == 0
        assert clean_census_df["total_population"].isna().sum() == 0



# 3. Data Cleaning (OpenAQ Locations)


class TestCleanOpenAQLocations:

    def test_irrelevant_parameters_filtered_out(self, clean_locations_df):
        """Only pm25, o3, no2 should remain after cleaning."""
        assert set(clean_locations_df["parameter"].unique()).issubset(set(RELEVANT_PARAMS))

    def test_duplicate_location_parameter_pairs_removed(self, raw_locations_df,
                                                         clean_locations_df):
        """Each (location_id, parameter) pair should appear at most once."""
        dupes = clean_locations_df.duplicated(subset=["location_id", "parameter"]).sum()
        assert dupes == 0, f"{dupes} duplicate location-parameter pairs remain"

    def test_coordinates_within_boston_bbox(self, clean_locations_df):
        """All retained stations must fall within the Boston bounding box."""
        assert clean_locations_df["latitude"].between(42.2, 42.75).all()
        assert clean_locations_df["longitude"].between(-71.25, -70.70).all()



# 4. Data Cleaning (OpenAQ Measurements)


class TestCleanOpenAQMeasurements:

    def test_negative_values_removed(self, clean_measurements_df):
        """avg_value must be >= 0 after cleaning."""
        assert (clean_measurements_df["avg_value"] >= 0).all()

    def test_only_2024_records_retained(self, clean_measurements_df):
        """All records must be from 2024."""
        assert (clean_measurements_df["date"].dt.year == 2024).all()

    def test_duplicate_station_parameter_date_removed(self, clean_measurements_df):
        """No duplicate (location_id, parameter, date) triplets should remain."""
        dupes = clean_measurements_df.duplicated(
            subset=["location_id", "parameter", "date"]
        ).sum()
        assert dupes == 0, f"{dupes} duplicate station-parameter-date rows remain"

    def test_month_column_added(self, clean_measurements_df):
        """month and month_name columns must be present and valid."""
        assert "month" in clean_measurements_df.columns
        assert "month_name" in clean_measurements_df.columns
        assert clean_measurements_df["month"].between(1, 12).all()


# 5. Merge Logic (Census aggregation)

class TestAggregateCensusToCounty:

    def test_output_has_one_row_per_county(self, census_county_df):
        """Aggregated census output must have exactly one row per county."""
        assert census_county_df["county_fips"].nunique() == len(census_county_df)

    def test_total_population_preserved(self, clean_census_df, census_county_df):
        """Sum of county populations must equal sum of tract populations."""
        assert census_county_df["total_population"].sum() == \
               clean_census_df["total_population"].sum()

    def test_county_names_mapped_correctly(self, census_county_df):
        """county_name must match the COUNTY_FIPS_TO_NAME reference."""
        for _, row in census_county_df.iterrows():
            expected = COUNTY_FIPS_TO_NAME.get(row["county_fips"])
            if expected is not None:
                assert row["county_name"] == expected, \
                    f"FIPS {row['county_fips']} mapped to {row['county_name']!r}, " \
                    f"expected {expected!r}"


# 6. Merge Logic (GHGRP aggregation)

class TestAggregateGHGRPToCounty:

    def test_ghgrp_aggregation_schema(self):
        """aggregate_ghgrp_to_county must produce the expected columns."""
        raw = pd.DataFrame({
            "ghgrp_id":         [1, 2, 3],
            "county":           ["Suffolk County", "Suffolk County", "Middlesex County"],
            "ghg_quantity_co2e":[100000.0, 200000.0, 50000.0],
            "state":            ["MA", "MA", "MA"],
        })
        result = aggregate_ghgrp_to_county(raw)
        expected_cols = {
            "county_name", "ghgrp_facility_count",
            "ghgrp_total_co2e", "ghgrp_mean_facility_co2e", "ghgrp_max_facility_co2e"
        }
        assert expected_cols.issubset(set(result.columns))

    def test_ghgrp_county_strip_removes_suffix(self):
        """standardize_county_name should strip ' County' suffix."""
        series = pd.Series(["Suffolk County", "Middlesex COUNTY", "Norfolk"])
        result = standardize_county_name(series)
        assert "County" not in " ".join(result.dropna().tolist())
        assert result.iloc[2] == "Norfolk"


# 7. Merge Logic (Static county dataset)

class TestBuildStaticCountyDataset:

    def test_merge_preserves_all_census_rows(self, census_county_df, ghgrp_county_df):
        """Left join on Census must retain all Census counties."""
        result = build_static_county_dataset(census_county_df, ghgrp_county_df)
        assert len(result) == len(census_county_df)

    def test_missing_ghgrp_counties_filled_with_zero(self, census_county_df,
                                                       ghgrp_county_df):
        """Counties without GHGRP data must have 0, not NaN, for emission cols."""
        result = build_static_county_dataset(census_county_df, ghgrp_county_df)
        fill_cols = [
            "ghgrp_facility_count", "ghgrp_total_co2e",
            "ghgrp_mean_facility_co2e", "ghgrp_max_facility_co2e",
        ]
        for col in fill_cols:
            if col in result.columns:
                assert result[col].isna().sum() == 0, \
                    f"NaN found in {col} — should be filled with 0"


# 8. Data Validation

class TestDataValidation:

    def test_openaq_monthly_aggregation_structure(
        self, clean_locations_df, clean_measurements_df
    ):
        """aggregate_openaq_monthly must produce required columns."""
        result = aggregate_openaq_monthly(clean_locations_df, clean_measurements_df)
        required = {
            "location_id", "parameter", "month",
            "monthly_avg_value", "days_observed"
        }
        assert required.issubset(set(result.columns))

    def test_days_observed_is_positive(self, clean_locations_df, clean_measurements_df):
        """Every station-month-parameter group must have at least 1 day observed."""
        result = aggregate_openaq_monthly(clean_locations_df, clean_measurements_df)
        assert (result["days_observed"] > 0).all()

    def test_safe_weighted_avg_ignores_nan_weights(self):
        """safe_weighted_avg must handle NaN weights without crashing."""
        values  = pd.Series([10.0, 20.0, 30.0])
        weights = pd.Series([np.nan, 2.0, 3.0])
        result = safe_weighted_avg(values, weights)
        assert not np.isnan(result), "Result should not be NaN when valid pairs exist"
        assert result == pytest.approx(26.0)   # (20*2 + 30*3) / (2+3)

    def test_safe_weighted_avg_all_nan_returns_nan(self):
        """safe_weighted_avg must return NaN when all weights are NaN."""
        values  = pd.Series([10.0, 20.0])
        weights = pd.Series([np.nan, np.nan])
        result = safe_weighted_avg(values, weights)
        assert np.isnan(result)

    def test_clean_measurements_avg_value_is_numeric(self, clean_measurements_df):
        """avg_value must be a numeric dtype after cleaning."""
        assert pd.api.types.is_numeric_dtype(clean_measurements_df["avg_value"])

    def test_census_population_is_positive_after_cleaning(self, clean_census_df):
        """All remaining tracts must have total_population > 0."""
        assert (clean_census_df["total_population"] > 0).all()