import pandas as pd
import plotly.express as px
import plotly.graph_objects as go



MERGED_STATIC_PATH = "data/processed/merged_county_static.csv"
MERGED_MONTHLY_PATH = "data/processed/merged_county_monthly.csv"
OPENAQ_STATION_MONTHLY_PATH = "data/processed/openaq_station_monthly.csv"


#  Scatter: Air Quality vs. Socioeconomic Variable

# available x-axis options for the dashboard dropdown
SCATTER_X_OPTIONS = {
    "Median Household Income": "median_household_income_weighted",
    "% Drive to Work": "pct_drive_weighted",
    "% Public Transit": "pct_public_transit_weighted",
    "% Minority": "pct_minority_weighted",
    "% No Vehicle": "pct_no_vehicle_weighted",
}

# available pollutant options
POLLUTANT_OPTIONS = ["pm25", "o3", "no2"]

# display-friendly labels
POLLUTANT_LABELS = {
    "pm25": "PM2.5 (µg/m³)",
    "o3": "Ozone (ppm)",
    "no2": "NO₂ (ppm)",
}


def build_scatter(
    static_df: pd.DataFrame,
    monthly_df: pd.DataFrame,
    x_variable: str = "median_household_income_weighted",
    pollutant: str = "pm25",
) -> go.Figure:
    """Build a scatter plot: socioeconomic variable vs. mean air quality by county.

    Args:
        static_df:  merged_county_static DataFrame (one row per county)
        monthly_df: merged_county_monthly DataFrame (one row per county-month-parameter)
        x_variable: column name from static_df to use as x-axis
        pollutant:  one of 'pm25', 'o3', 'no2'

    Returns:
        Plotly Figure
    """
    # compute annual mean air quality per county for the selected pollutant
    aq_annual = (
        monthly_df[monthly_df["parameter"] == pollutant]
        .groupby("county_name", as_index=False)
        .agg(mean_air_quality=("air_quality_mean", "mean"))
    )

    # merge with static county data
    plot_df = static_df.merge(aq_annual, on="county_name", how="inner")

    # find friendly label for x-axis
    x_label = next(
        (label for label, col in SCATTER_X_OPTIONS.items() if col == x_variable),
        x_variable,
    )
    y_label = POLLUTANT_LABELS.get(pollutant, pollutant)

    fig = px.scatter(
        plot_df,
        x=x_variable,
        y="mean_air_quality",
        size="total_population",
        color="ghgrp_total_co2e",
        color_continuous_scale="OrRd",
        hover_name="county_name",
        hover_data={
            x_variable: ":.1f",
            "mean_air_quality": ":.4f",
            "total_population": ":,",
            "ghgrp_total_co2e": ":,.0f",
            "ghgrp_facility_count": True,
        },
        labels={
            x_variable: x_label,
            "mean_air_quality": f"Mean {y_label}",
            "total_population": "Population",
            "ghgrp_total_co2e": "GHGRP Emissions (CO₂e tons)",
            "ghgrp_facility_count": "GHGRP Facilities",
        },
        title=f"Air Quality vs. {x_label} by County",
    )

    fig.update_layout(
        template="plotly_white",
        height=500,
        coloraxis_colorbar_title="GHGRP<br>Emissions",
    )

    # add county name annotations next to each point
    for _, row in plot_df.iterrows():
        fig.add_annotation(
            x=row[x_variable],
            y=row["mean_air_quality"],
            text=row["county_name"],
            showarrow=False,
            yshift=15,
            font=dict(size=11),
        )

    return fig


#  Station-Level Monthly Heatmap

def build_heatmap(
    station_monthly_df: pd.DataFrame,
    pollutant: str = "pm25",
) -> go.Figure:
    """Build a heatmap: stations (rows) × months (columns), colored by monthly avg.

    Args:
        station_monthly_df: openaq_station_monthly DataFrame
                            (one row per station-month-parameter)
        pollutant:          one of 'pm25', 'o3', 'no2'

    Returns:
        Plotly Figure
    """
    # filter to selected pollutant
    df = station_monthly_df[station_monthly_df["parameter"] == pollutant].copy()

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text=f"No data for {pollutant}", showarrow=False)
        return fig

    # pivot: rows = station, columns = month, values = monthly avg
    pivot = df.pivot_table(
        index="location_name",
        columns="month",
        values="monthly_avg_value",
        aggfunc="mean",
    )

    # ensure all 12 months are present as columns
    for m in range(1, 13):
        if m not in pivot.columns:
            pivot[m] = float("nan")
    pivot = pivot[sorted(pivot.columns)]

    # month labels for x-axis
    month_labels = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]

    # sort stations alphabetically
    pivot = pivot.sort_index(ascending=True)

    # build days-observed matrix for hover text
    days_pivot = df.pivot_table(
        index="location_name",
        columns="month",
        values="days_observed",
        aggfunc="sum",
    )
    for m in range(1, 13):
        if m not in days_pivot.columns:
            days_pivot[m] = 0
    days_pivot = days_pivot[sorted(days_pivot.columns)]
    days_pivot = days_pivot.reindex(pivot.index)

    # build custom hover text
    hover_text = []
    for station in pivot.index:
        row_text = []
        for m in pivot.columns:
            val = pivot.loc[station, m]
            days = days_pivot.loc[station, m] if station in days_pivot.index else 0
            if pd.isna(val):
                row_text.append(f"{station}<br>{month_labels[m-1]}<br>No data")
            else:
                row_text.append(
                    f"{station}<br>{month_labels[m-1]}<br>"
                    f"Avg: {val:.4f}<br>Days: {int(days)}"
                )
        hover_text.append(row_text)

    y_label = POLLUTANT_LABELS.get(pollutant, pollutant)

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=month_labels,
        y=pivot.index.tolist(),
        colorscale="Viridis",
        hoverinfo="text",
        text=hover_text,
        colorbar=dict(title=y_label),
        xgap=2,
        ygap=2,
    ))

    fig.update_layout(
        title=f"Monthly {y_label} by Station (2024)",
        xaxis_title="Month",
        yaxis_title="Station",
        template="plotly_white",
        height=max(400, len(pivot) * 35 + 100),  # scale height to station count
        yaxis=dict(tickfont=dict(size=10)),
    )

    return fig

def main():
    static_df = pd.read_csv(MERGED_STATIC_PATH)
    monthly_df = pd.read_csv(MERGED_MONTHLY_PATH)
    station_monthly_df = pd.read_csv(OPENAQ_STATION_MONTHLY_PATH)

    # Scatter
    scatter_fig = build_scatter(
        static_df,
        monthly_df,
        x_variable="median_household_income_weighted",
        pollutant="pm25",
    )
    scatter_fig.show()

    # heatmap
    heatmap_fig = build_heatmap(station_monthly_df, pollutant="pm25")
    heatmap_fig.show()


if __name__ == "__main__":
    main()