# Boston Air Equity: Design Document

## Project Domain
Our project examines environmental equity in the Boston metropolitan area by connecting transportation behavior, socioeconomic inequality, and air quality outcomes. We are specifically interested in whether communities that rely more heavily on cars and use public transit less tend to experience worse pollution exposure, and whether these patterns disproportionately affect lower-income communities.

## Research Question
In the Boston metropolitan area, do neighborhoods with higher car dependency and lower public transit usage experience worse air quality, and are these burdens disproportionately concentrated in lower-income communities?

## Data Sources
1. OpenAQ API: provides air quality monitoring station metadata and pollutant measurements where we will collect monitoring station coordinates, pollutant type, and daily average pollutant values for the Boston area.
2. Census ACS API: provides tract-level demographic, transportation, and income indicators where we will retrieve variables including total population, median household income, vehicle availability, commute mode, and race/ethnicity, and derive percentages for no-vehicle households, public transit commuters, car commuters, and minority population share
3. Static dataset: provides the polygon boundaries needed to spatially assign air quality monitors to a geographic unit that can be merged with Census data

## Key Fields
- OpenAQ: location_id, sensor_id, latitude, longitude, parameter, date, and avg_value
- ACS: GEOID, median_household_income, pct_no_vehicle, pct_public_transit, pct_drive, pct_minority, and total_population
- Static dataset: GEOID

## Merge Strategy
The project combines point-level air quality data with tract-level Census data. OpenAQ provides monitor coordinates and daily pollutant readings, while ACS provides demographic and transportation indicators by tract. Because these datasets operate at different geographic levels, we will use a static tract or neighborhood boundary dataset to bridge them. Monitoring stations will be spatially assigned to a tract or neighborhood using latitude and longitude. We will then aggregate air quality readings to that same geographic level and merge the resulting air quality summaries with ACS variables using GEOID or another shared geographic identifier.

## Proposed Architecture
The pipeline begins with data acquisition from OpenAQ and the Census ACS API, along with loading the static boundary dataset. Each source will then be cleaned and standardized separately. Next, air quality monitor locations will be spatially mapped to tracts or neighborhoods. Daily pollutant values will be aggregated by geography and merged with Census demographic and transportation indicators. The cleaned integrated dataset will then be saved for use in a Panel dashboard, where users will explore spatial and socioeconomic patterns in pollution exposure.

## Anticipated Challenges
A key challenge is that air quality monitors are point-based and may not be evenly distributed across all Boston-area tracts. Some tracts may have no directly assigned monitor, which may require aggregation to a broader geography or careful filtering. Another challenge is aligning temporal air quality readings with ACS data, since ACS values are multi-year estimates while air quality data is daily. We will address this by aggregating air quality data over a clearly defined study period.
