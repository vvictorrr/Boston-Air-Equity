# Boston Air Equity: Design Document

## Project Domain
Our project examines environmental equity in the Boston metropolitan area by connecting transportation behavior, socioeconomic inequality, and air quality outcomes. We are specifically interested in whether communities that rely more heavily on cars and use public transit less tend to experience worse pollution exposure, and whether these patterns disproportionately affect lower-income communities.

## Research Question
In the Boston metropolitan area, do neighborhoods with higher car dependency and lower public transit usage experience worse air quality, and are these burdens disproportionately concentrated in lower-income communities?

## Data Sources
### 1. OpenAQ API
OpenAQ provides air quality monitoring station metadata and pollutant measurements. For this project, we will collect Boston-area monitoring station coordinates, pollutant type, and daily average pollutant values. Our acquisition script currently fetches location metadata and daily measurements for pollutants such as PM2.5, ozone, and NO2.

### 2. Census ACS API
The Census ACS API provides tract-level demographic, transportation, and income indicators. Our script retrieves variables including total population, median household income, vehicle availability, commute mode, and race/ethnicity. It also derives useful percentages such as no-vehicle households, public transit commuters, car commuters, and minority population share, and constructs a tract-level GEOID for geographic merging.

### 3. EPA GHGRP Static Dataset
The EPA Greenhouse Gas Reporting Program (GHGRP) dataset will allow us to incorporate facility-level emissions context into our analysis of environmental burden in the Boston metropolitan area.

## Documentation and Access Notes
The project notes include documentation and access references for all three sources. The Census API notes include the ACS 5-year dataset documentation and API guidance. The OpenAQ notes include API documentation for locations and measurements, along with rate-limit guidance. The GHGRP notes include the EPA data portal and dataset documentation.

## Key Fields
### OpenAQ
- `location_id`
- `sensor_id`
- `latitude`
- `longitude`
- `parameter`
- `date`
- `avg_value` :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9}

### Census ACS
- `GEOID`
- `median_household_income`
- `pct_no_vehicle`
- `pct_public_transit`
- `pct_drive`
- `pct_minority`
- `total_population` :contentReference[oaicite:10]{index=10}

### GHGRP
The exact fields will depend on the final subset we download from the EPA GHGRP dataset, but we expect to use geographic and emissions-related fields to connect facility emissions context to the Boston metro area. At minimum, this source will contribute facility or emissions information relevant to the environment.


## Merge Strategy
The project combines three sources that operate at different levels of geography. OpenAQ provides point-level air quality monitor data, Census ACS provides tract-level socioeconomic and transportation indicators, and GHGRP provides a static environmental emissions dataset. Our primary unit of social analysis will be Census tracts, since ACS already provides tract-level measures of income, commuting behavior, and demographics.

We will first clean each source separately. For ACS, the tract GEOID already provides a geographic identifier. For OpenAQ, monitor coordinates will be used to assign air quality observations to the Boston-area geography used in the analysis. For GHGRP, we will use the geographic information available in the static dataset to identify relevant facilities in or near the Boston metropolitan area and connect them to the study geography. After geographic alignment, we will aggregate air quality and emissions-related measures as needed and merge them with ACS tract-level indicators for analysis.

## Proposed Architecture
The pipeline begins with data acquisition from the OpenAQ API and the Census ACS API, plus loading the static GHGRP dataset. Each source will then be cleaned and standardized separately. OpenAQ air quality readings will be filtered to the Boston metropolitan area and summarized over the study period. ACS data will be processed into tract-level transportation, income, and demographic indicators. GHGRP data will be filtered to the relevant geography and cleaned for environmental emissions context. The cleaned datasets will then be geographically aligned and merged into a unified analytical dataset. This final dataset will be used in a Panel dashboard to explore spatial and socioeconomic patterns in air quality and environmental burden.

## Anticipated Challenges
One challenge is that the three datasets are not naturally stored at the same geographic level. OpenAQ measurements are point-based, ACS is tract-based, and GHGRP may use facility-based records. This means that geographic alignment and aggregation will be an important part of the project design.

Another challenge is uneven data coverage. Air quality monitors may not exist in every tract, and GHGRP facilities may be concentrated in only certain parts of the region. This could limit direct comparisons across all neighborhoods.

A third challenge is temporal alignment. ACS values are multi-year estimates, while OpenAQ measurements are daily and GHGRP reporting may follow a different reporting schedule. To address this, we will define a clear study period and use aggregated summaries where appropriate.
