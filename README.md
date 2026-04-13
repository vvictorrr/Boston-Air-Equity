# Boston-Air-Equity
 
Analyzing the relationship between air quality, car dependency, public transit usage, and socioeconomic factors across Boston-area neighborhoods.
 
**Research Question:** In the Boston metro area, do neighborhoods with higher car dependency and lower public transit usage experience worse air quality, and do these patterns disproportionately affect lower-income communities?

The project follows a layered architecture with strict separation of concerns. The pipeline currently flows as:
fetch_data.py -> clean.py -> merge.py

fetch_data.py saves raw data to CSV files in "data/raw/", which the downstream layers read from. This caching step avoids hitting API rate limits on every run. Then, clean.py reads these raw CSVs and returns cleaned DataFrames. merge.py imports cleaning functions from clean.py and runs them on the raw files, and merges the results. 

## Data Sources
 
### 1. OpenAQ API v3 (API)
Real-time and historical air quality monitoring data from stations across the Boston metro area. We pull daily average readings for PM2.5, O3, and NO2 from 14 monitoring stations over the full year of 2024.
 
- Docs: https://docs.openaq.org
- Key fields: `location_id`, `location_name`, `latitude`, `longitude`, `parameter`, `date`, `avg_value`
- Authentication: API key via `X-API-Key` header (register at https://explore.openaq.org/register)
 
### 2. Census Bureau ACS 5-Year Estimates (API)
Tract-level socioeconomic and transportation data for four Boston-area counties (Suffolk, Middlesex, Norfolk, Essex) from the 2023 ACS 5-year estimates.
 
- Docs: https://api.census.gov/data/2023/acs/acs5/variables.html
- Key tables: B01003 (population), B19013 (median income), B25044 (vehicle availability), B08301 (commute mode), B03002 (race/ethnicity)
- Authentication: API key (register at https://api.census.gov/data/key_signup.html)
 
### 3. EPA GHGRP FLIGHT (Static Dataset)
Facility-level greenhouse gas emissions data for Massachusetts, exported from EPA's FLIGHT tool. Provides locations and annual CO2-equivalent emissions for large industrial emitters.
 
- Source: https://ghgdata.epa.gov/flight/
- Key fields: `facility_name`, `latitude`, `longitude`, `county`, `ghg_quantity_co2e`
- Format: XLS file with 4 metadata header rows before the actual data

## Setup and Installation
 
### Prerequisites
- Python 3.10+
- conda or pip
 
### Steps
 
1. Clone the repository:
```bash
git clone https://github.com/vvictorrr/Boston-Air-Equity.git
cd Boston-Air-Equity
```
 
2. Ensure dependencies are installed. If not, run:
```bash
pip install pandas requests python-dotenv plotly panel pytest openpyxl
```
 
3. Set up API keys:
- Register for an OpenAQ API key at https://explore.openaq.org/register
- Register for a Census API key at https://api.census.gov/data/key_signup.html
```bash
touch .env
echo "OPENAQ_API_KEY=your_key_here\nCENSUS_API_KEY=your_key_here" > .env
```
Replace your_key_here with your respective API keys

4. Fetch raw data:
```bash
python fetch_data.py
```
 
5. Run the full pipeline (clean + merge):
```bash
python merge.py
```
 
6. Run tests:
```bash
pytest test.py -v
```


## Data Cleaning Decisions
 
### Census ACS (`clean_census`)
 
| Issue | Decision | Justification |
|-------|----------|---------------|
| Sentinel value `-666666666` | Replaced with `NaN` | Census Bureau uses this as a null indicator for suppressed data (e.g., tracts with too few respondents). Leaving it as a number would corrupt all aggregations. |
| Uninhabited tracts (population = 0) | Dropped | These are water bodies, parks, or institutional tracts with no residential population. They have no meaningful socioeconomic data and would distort county-level averages. |
| GEOID formatting | Zero-padded to 11 characters | Census GEOIDs must be exactly 11 digits (2 state + 3 county + 6 tract) to join correctly with shapefiles. Raw integer GEOIDs lose leading zeros. |
| Derived percentages (`pct_drive`, etc.) | Recalculated from raw counts | Some tracts had `NaN` in percentage columns despite having valid raw counts. Recalculating from counts ensures consistency. Division by zero (tracts with 0 commuters) produces `NaN`, which is correct. |
| Tracts missing income or population after cleaning | Dropped | These tracts cannot contribute to any analysis and would cause issues in weighted averages. |
 
### OpenAQ Locations (`clean_openaq_locations`)
 
| Issue | Decision | Justification |
|-------|----------|---------------|
| Non-relevant parameters (co, temperature, humidity, etc.) | Filtered to pm25, o3, no2 only | The research question focuses on criteria air pollutants linked to traffic and industrial emissions. Meteorological sensors and CO are not needed. |
| Duplicate sensors per location-parameter | Kept first only | Some stations have multiple sensors measuring the same pollutant. Keeping duplicates would double-count readings after merging with measurements. |
| Stations outside Boston metro area | Dropped (coordinate validation) | One station had coordinates at latitude 99.0, clearly erroneous. Bounding box filter catches these. |
 
### OpenAQ Measurements (`clean_openaq_measurements`)
 
| Issue | Decision | Justification |
|-------|----------|---------------|
| Negative `avg_value` readings | Removed (set threshold at 0) | Minimum value in raw data was -0.0003. Negative concentrations are instrument noise or calibration artifacts, not real measurements. |
| Records outside 2024 | Dropped | One record dated 2025-01-01 was present. Trimming to 2024 ensures a clean calendar-year analysis window. |
| Duplicate station-parameter-date rows | Deduplicated (keep first) | Some stations had two identical readings for the same day and parameter, likely from reprocessing. |
| Month extraction | Added `month` and `month_name` columns | Required for the animated monthly visualization and for the county-month aggregation in the merge layer. |
 
### EPA GHGRP (`clean_ghgrp`)
 
| Issue | Decision | Justification |
|-------|----------|---------------|
| 4 metadata header rows + 1 blank row in XLS | Read with `header=5` | FLIGHT exports include descriptive text above the actual data table. Skipping these rows gives the correct column names. |
| Column names in ALL CAPS with spaces | Renamed to snake_case | Consistent naming convention across the codebase. |
| Facilities with missing coordinates or emissions | Dropped | Cannot be mapped or analyzed without location and emission quantity. |
| One facility with coordinates outside Massachusetts | Kept (state filter catches it) | Analog Devices has lat/lon pointing to Oregon despite `state=MA`. The state filter retains it since the EPA reported it as MA. The incorrect coordinates only matter if doing distance-based analysis, which we do at the county level instead. |
| County name formatting ("Suffolk County" vs "Suffolk") | Standardized via `standardize_county_name` | Strips " County" suffix and title-cases for consistent joins with Census county names. |
 
## Known Data Quality Issues
 
**Sparse monitoring coverage:** Only 14 OpenAQ stations cover four counties with 920 Census tracts. County-level aggregation mitigates this, but some counties may have only 1-2 stations, making their air quality averages less reliable.
 
**Missing station-county mapping:** OpenAQ stations do not include county information. The mapping file `openaq_station_county_map.csv` was created manually by looking up each station's coordinates. If a station falls near a county boundary, the assignment is a judgment call.
 
**Uneven temporal coverage:** Not all stations report every day. Some community-run sensors (e.g., Jamaica Plain, East Arlington) have significant gaps. The `days_observed` field in the monthly aggregation tracks this so the dashboard can flag low-coverage months.
 
**Mixed units in OpenAQ data:** PM2.5 is reported in µg/m³ while NO2 and O3 are in ppm. These are not directly comparable, so visualizations should always filter by parameter or clearly label units.
 
**GHGRP only covers large emitters:** The GHGRP threshold is 25,000 metric tons CO2e per year. Smaller facilities and mobile sources (vehicle traffic), which are likely the dominant pollution sources in urban Boston, are not captured. The GHGRP data shows industrial emission hotspots but does not represent the full pollution picture.
 
**GHGRP temporal mismatch:** GHGRP facility emissions are from the reporting year 2023, while OpenAQ air quality measurements are from 2024 and Census ACS estimates span 2019–2023. We treat emissions as roughly stable year-to-year for the purpose of county-level comparison, but any facility that opened, closed, or significantly changed output between 2023 and 2024 would not be reflected.
 
**Census ACS margin of error:** ACS 5-year estimates for small tracts can have wide margins of error, especially for detailed tables like vehicle availability. County-level aggregation reduces this issue by pooling across many tracts.
 
