import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import panel as pn
import numpy as np

pn.extension("plotly", sizing_mode="stretch_width")

pn.extension(
    raw_css=["""
    :root {
        --bg:      #f0f4f8;
        --surface: #ffffff;
        --accent:  #1d4ed8;
        --text:    #0f172a;
        --radius:  10px;
        --shadow:  0 2px 14px rgba(0,0,0,.07);
    }
    body { background: var(--bg); }
    .card {
        background: var(--surface);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        padding: 18px 22px;
    }
    .dash-header {
        background: linear-gradient(120deg, #0f2d5e 0%, #1d4ed8 55%, #0ea5e9 100%);
        border-radius: var(--radius);
        padding: 26px 36px 20px;
        color: #fff;
    }
    .dash-header h1 { margin: 0 0 4px; font-size: 1.8rem; font-weight: 700; letter-spacing: -.4px; }
    .dash-header p  { margin: 0; opacity: .78; font-size: .9rem; }
    .sec-title {
        font-weight: 700; font-size: 1rem; color: var(--text);
        border-left: 4px solid var(--accent);
        padding-left: 10px; margin: 6px 0 10px;
    }
    """]
)


MERGED_STATIC_PATH = "data/processed/merged_county_static.csv"
MERGED_MONTHLY_PATH = "data/processed/merged_county_monthly.csv"
OPENAQ_STATION_MONTHLY_PATH = "data/processed/openaq_station_monthly.csv"


SCATTER_X_OPTIONS = {
    "Median Household Income": "median_household_income_weighted",
    "% Drive to Work": "pct_drive_weighted",
    "% Public Transit": "pct_public_transit_weighted",
    "% Minority": "pct_minority_weighted",
    "% No Vehicle": "pct_no_vehicle_weighted",
}

# available pollutant options
POLLUTANT_OPTIONS = ["pm25", "o3", "no2"]

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
    aq_annual = (
        monthly_df[monthly_df["parameter"] == pollutant]
        .groupby("county_name", as_index=False)
        .agg(mean_air_quality=("air_quality_mean", "mean"))
    )

    plot_df = static_df.merge(aq_annual, on="county_name", how="inner")

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
        color_continuous_scale="agsunset",
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

    fig.update_layout(template="plotly_white", height=500)

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


def build_heatmap(
    station_monthly_df: pd.DataFrame,
    pollutant: str = "pm25",
) -> go.Figure:
    df = station_monthly_df[station_monthly_df["parameter"] == pollutant].copy()

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text=f"No data for {pollutant}", showarrow=False)
        return fig

    pivot = df.pivot_table(
        index="location_name",
        columns="month",
        values="monthly_avg_value",
        aggfunc="mean",
    )

    for m in range(1, 13):
        if m not in pivot.columns:
            pivot[m] = float("nan")
    pivot = pivot[sorted(pivot.columns)]

    month_labels = ["Jan","Feb","Mar","Apr","May","Jun",
                    "Jul","Aug","Sep","Oct","Nov","Dec"]

    pivot = pivot.sort_index(ascending=True)

    zmin = np.nanpercentile(pivot.values, 2)
    zmax = np.nanpercentile(pivot.values, 99)

    fig = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=month_labels,
        y=pivot.index.tolist(),
        colorscale="Viridis",
        zmin=zmin,
        zmax=zmax,
    ))

    fig.update_layout(
        title=f"Monthly {pollutant.upper()} by Station (2024)",
        template="plotly_white",
        height=500,
    )

    return fig


# Load data
static_df          = pd.read_csv(MERGED_STATIC_PATH)
monthly_df         = pd.read_csv(MERGED_MONTHLY_PATH)
station_monthly_df = pd.read_csv(OPENAQ_STATION_MONTHLY_PATH)


MONTH_MAP = {
    1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr",  5:"May",  6:"Jun",
    7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec",
}

# Consistent color per county across all charts
COUNTY_COLORS = {
    county: px.colors.qualitative.Plotly[i]
    for i, county in enumerate(sorted(monthly_df["county_name"].unique()))
}

# Pollutant dropdown shows uppercase labels, passes lowercase values
POLLUTANT_SELECT_OPTIONS = {"PM25": "pm25", "O3": "o3", "NO2": "no2"}


# compute slider bounds from annual means for a pollutant
def _aq_bounds(pollutant):
    df = (
        monthly_df[monthly_df["parameter"] == pollutant]
        .groupby("county_name")["air_quality_mean"]
        .mean()
    )
    hi = round(float(df.max()) + 0.5, 4)
    return 0.0, hi

# Widgets
pollutant_select = pn.widgets.Select(
    name="Pollutant",
    options=POLLUTANT_SELECT_OPTIONS,
    value="pm25",
)

x_select = pn.widgets.Select(
    name="Socioeconomic Variable",
    options=SCATTER_X_OPTIONS,
    value="median_household_income_weighted",
)

_init_lo, _init_hi = _aq_bounds("pm25")

aq_range_slider = pn.widgets.RangeSlider(
    name="Filter by Mean AQ Range",
    start=_init_lo,
    end=_init_hi,
    value=(_init_lo, _init_hi),
    step=0.0001,
)

reset_button = pn.widgets.Button(name="↺  Reset Filters", button_type="light")

def _update_slider_bounds(event):
    lo, hi = _aq_bounds(event.new)
    aq_range_slider.start = lo
    aq_range_slider.end   = hi
    aq_range_slider.step  = round((hi - lo) / 100, 6)
    aq_range_slider.value = (lo, hi)

pollutant_select.param.watch(_update_slider_bounds, "value")

def reset_widgets(event):
    pollutant_select.value = "pm25"
    x_select.value         = "median_household_income_weighted"
    lo, hi = _aq_bounds("pm25")
    aq_range_slider.start = lo
    aq_range_slider.end   = hi
    aq_range_slider.value = (lo, hi)

reset_button.on_click(reset_widgets)

def sec(title):
    return pn.pane.HTML(
        f'<div class="sec-title">{title}</div>',
        sizing_mode="stretch_width",
    )


# KPI metrics
@pn.depends(pollutant_select)
def kpi_row(pollutant):
    df = monthly_df[monthly_df["parameter"] == pollutant]
    return pn.Row(
        pn.indicators.Number(
            name="Avg AQ",
            value=round(df["air_quality_mean"].mean(), 4),
            format="{value:.4f}",
            font_size="28pt", title_size="12pt",
        ),
        pn.indicators.Number(
            name="Max AQ",
            value=round(df["air_quality_mean"].max(), 4),
            format="{value:.4f}",
            font_size="28pt", title_size="12pt",
        ),
        pn.indicators.Number(
            name="Counties Tracked",
            value=df["county_name"].nunique(),
            format="{value}",
            font_size="28pt", title_size="12pt",
        ),
        pn.indicators.Number(
            name="Monitoring Stations",
            value=station_monthly_df["location_name"].nunique(),
            format="{value}",
            font_size="28pt", title_size="12pt",
        ),
        pn.indicators.Number(
            name="Total CO₂e (tons)",
            value=int(static_df["ghgrp_total_co2e"].sum()),
            format="{value:,}",
            font_size="28pt", title_size="12pt",
        ),
        sizing_mode="stretch_width",
    )


# Vis 1:  High-level overview, ranked bar chart
@pn.depends(pollutant_select, aq_range_slider)
def overview_bar(pollutant, aq_range):
    df = (
        monthly_df[monthly_df["parameter"] == pollutant]
        .groupby("county_name", as_index=False)
        .agg(mean_aq=("air_quality_mean", "mean"))
    )
    df = df[(df["mean_aq"] >= aq_range[0]) & (df["mean_aq"] <= aq_range[1])]

    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="No counties in selected AQ range", showarrow=False)
        return fig

    df = df.sort_values("mean_aq", ascending=False)
    y_label = POLLUTANT_LABELS.get(pollutant, pollutant)

    fig = go.Figure()
    for _, row in df.iterrows():
        fig.add_trace(go.Bar(
            x=[row["mean_aq"]],
            y=[row["county_name"]],
            orientation="h",
            marker_color=COUNTY_COLORS.get(row["county_name"], "#888"),
            name=row["county_name"],
            text=[f"{row['mean_aq']:.4f}"],
            textposition="outside",
        ))

    fig.update_layout(
        title=f"Counties by Mean {pollutant.upper()} (Annual Avg)",
        xaxis_title=f"Mean {y_label}",
        yaxis={"categoryorder": "total ascending"},
        template="plotly_white",
        height=320,
        showlegend=False,
        margin=dict(l=10, r=30, t=50, b=30),
    )
    return fig


# Vis 2: scatter
@pn.depends(x_select, pollutant_select)
def scatter_view(x, pollutant):
    return build_scatter(static_df, monthly_df, x, pollutant)

# Vis 3: heatmap
@pn.depends(pollutant_select)
def heatmap_view(pollutant):
    return build_heatmap(station_monthly_df, pollutant)

# Vis 4: Animated line chart
def build_animated_fig(pollutant):
    df = (
        monthly_df[monthly_df["parameter"] == pollutant]
        .groupby(["county_name", "month"], as_index=False)
        .agg(mean_aq=("air_quality_mean", "mean"))
        .sort_values(["county_name", "month"])
    )

    counties    = sorted(df["county_name"].unique())
    months      = sorted(df["month"].unique())
    month_names = [MONTH_MAP[m] for m in months]
    y_label     = POLLUTANT_LABELS.get(pollutant, pollutant)

    ymin    = df["mean_aq"].min()
    ymax    = df["mean_aq"].max()
    ypad    = (ymax - ymin) * 0.15
    y_range = [ymin - ypad, ymax + ypad]

    first_month = months[0]
    traces = []
    for i, county in enumerate(counties):
        cdf = df[(df["county_name"] == county) & (df["month"] <= first_month)]
        traces.append(go.Scatter(
            x=[MONTH_MAP[m] for m in cdf["month"]],
            y=cdf["mean_aq"],
            mode="lines+markers",
            name=county,
            line=dict(color=COUNTY_COLORS.get(county, "#888"), width=2),
            marker=dict(size=7),
        ))

    frames = []
    for m in months:
        frame_traces = []
        for i, county in enumerate(counties):
            cdf = df[(df["county_name"] == county) & (df["month"] <= m)]
            frame_traces.append(go.Scatter(
                x=[MONTH_MAP[mo] for mo in cdf["month"]],
                y=cdf["mean_aq"],
                mode="lines+markers",
                name=county,
                line=dict(color=COUNTY_COLORS.get(county, "#888"), width=2),
                marker=dict(size=7),
            ))
        frames.append(go.Frame(data=frame_traces, name=str(m)))

    fig = go.Figure(data=traces, frames=frames)

    fig.update_layout(
        title=f"{pollutant.upper()} Monthly Trend by County — Animated",
        template="plotly_white",
        height=700,
        margin=dict(l=70, r=80, t=60, b=20),
        xaxis=dict(
            title="Month",
            categoryorder="array",
            categoryarray=month_names,
            range=[-0.5, len(month_names) - 0.5],
            domain=[0, 1],
            anchor="y",
        ),
        yaxis=dict(
            title=f"Mean {y_label}",
            domain=[0.42, 1.0],
            range=y_range,
        ),
        legend=dict(
            orientation="h",
            x=0.0,
            y=0.36,
            xanchor="left",
            yanchor="top",
        ),
        sliders=[{
            "steps": [
                {
                    "args": [[str(m)], {
                        "frame": {"duration": 300, "redraw": True},
                        "mode": "immediate",
                    }],
                    "label": MONTH_MAP[m],
                    "method": "animate",
                }
                for m in months
            ],
            "x": 0.05,
            "len": 0.90,
            "y": 0.22,
            "yanchor": "top",
            "currentvalue": {
                "prefix": "Month: ",
                "visible": True,
                "xanchor": "center",
            },
            "pad": {"t": 10, "b": 10},
        }],
        updatemenus=[{
            "type": "buttons",
            "showactive": False,
            "x": 0.5,
            "y": 0.08,
            "xanchor": "center",
            "yanchor": "top",
            "buttons": [
                {
                    "label": "▶  Play",
                    "method": "animate",
                    "args": [None, {
                        "frame": {"duration": 700, "redraw": True},
                        "fromcurrent": True,
                        "transition": {"duration": 300},
                    }],
                },
                {
                    "label": "⏸  Pause",
                    "method": "animate",
                    "args": [[None], {
                        "frame": {"duration": 0, "redraw": False},
                        "mode": "immediate",
                        "transition": {"duration": 0},
                    }],
                },
            ],
        }],
    )
    return fig


@pn.depends(pollutant_select)
def animated_lines(pollutant):
    fig = build_animated_fig(pollutant)
    html_str = fig.to_html(full_html=True, include_plotlyjs="cdn")
    escaped  = html_str.replace("&", "&amp;").replace('"', "&quot;")
    iframe   = (
        f'<iframe srcdoc="{escaped}" '
        f'style="width:100%;height:720px;border:none;"></iframe>'
    )
    return pn.pane.HTML(iframe, sizing_mode="stretch_width", height=720)


# Layout
header = pn.pane.HTML("""
<div class="dash-header">
  <h1>🌎 Air Quality &amp; Equity Dashboard</h1>
  <p>County-level pollutant monitoring · Socioeconomic correlations · 2024</p>
</div>
""", sizing_mode="stretch_width")

controls = pn.Column(
    sec("Filters"),
    pollutant_select,
    pn.pane.HTML('<hr style="border:none;border-top:1px solid #e2e8f0;margin:6px 0">'),
    x_select,
    pn.pane.HTML('<hr style="border:none;border-top:1px solid #e2e8f0;margin:6px 0">'),
    aq_range_slider,
    pn.pane.HTML('<hr style="border:none;border-top:1px solid #e2e8f0;margin:6px 0">'),
    reset_button,
    css_classes=["card"],
    width=280,
    margin=(0, 14, 0, 0),
)

dashboard = pn.Column(
    header,
    pn.layout.Divider(),

    # KPIs
    pn.Column(
        sec("📈 Key Metrics"),
        kpi_row,
        css_classes=["card"],
        sizing_mode="stretch_width",
    ),
    pn.layout.Divider(),

    # Overview bar
    sec("County Overview — Filter by Mean AQ Range"),
    pn.Row(
        controls,
        pn.Column(overview_bar, css_classes=["card"], sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
    ),
    pn.layout.Divider(),

    # Scatter + heatmap
    sec("Socioeconomic Correlations & Station Heatmap"),
    pn.Row(
        pn.Column(scatter_view, css_classes=["card"], sizing_mode="stretch_width"),
        pn.Column(heatmap_view, css_classes=["card"], sizing_mode="stretch_width"),
        sizing_mode="stretch_width",
    ),
    pn.layout.Divider(),

    # Animated
    sec("Animated Monthly Trends — Press Play or drag the slider"),
    pn.Column(animated_lines, css_classes=["card"], sizing_mode="stretch_width"),

    sizing_mode="stretch_width",
    max_width=1400,
    margin=(14, 20),
)

dashboard.show()