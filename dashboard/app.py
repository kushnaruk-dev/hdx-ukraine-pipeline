import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from db import get_engine, query

# ── Page config ───────────────────────────────────────────────
st.set_page_config(
    page_title="Ukraine Humanitarian Dashboard",
    page_icon="🇺🇦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme overrides ───────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background: #090d12; }
    [data-testid="stSidebar"]          { background: #0e1419; border-right: 1px solid #1e2d3d; }
    [data-testid="stMetric"]           { background: #0e1419; border: 1px solid #1e2d3d; border-radius: 8px; padding: 12px; }
    h1, h2, h3                         { color: #e8f0f8 !important; }
    .stSelectbox label, .stMultiSelect label { color: #7a8fa6 !important; font-size: 0.8rem; }
    div[data-testid="stMetricValue"]   { color: #00c896 !important; font-size: 1.8rem !important; }
    div[data-testid="stMetricLabel"]   { color: #7a8fa6 !important; }
    div[data-testid="stMetricDelta"]   { font-size: 0.8rem !important; }
</style>
""", unsafe_allow_html=True)

PLOTLY_THEME = dict(
    paper_bgcolor="#090d12",
    plot_bgcolor="#0e1419",
    font_color="#cdd6e0",
    colorway=["#00c896", "#4f8ef7", "#f7a14f", "#e05c7a", "#a78bfa"],
    xaxis=dict(gridcolor="#1e2d3d", linecolor="#1e2d3d"),
    yaxis=dict(gridcolor="#1e2d3d", linecolor="#1e2d3d"),
)

# ── Engine ────────────────────────────────────────────────────
try:
    engine = get_engine()
except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🇺🇦 HDX Ukraine")
    st.markdown(
        "<p style='color:#7a8fa6;font-size:0.8rem;'>"
        "Humanitarian data pipeline<br>Powered by HDX · MS SQL Server"
        "</p>", unsafe_allow_html=True
    )
    st.divider()
    page = st.radio(
        "View",
        ["Overview", "Food Prices", "People Reached (5W)", "Funding Gap"],
        label_visibility="collapsed"
    )
    st.divider()
    st.markdown(
        "<p style='color:#3a4d5e;font-size:0.72rem;font-family:monospace;'>"
        "Data: HDX Public API<br>Pipeline: Python + SSMS<br>© Mykola Kushnaruk"
        "</p>", unsafe_allow_html=True
    )


# ══════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════
if page == "Overview":
    st.markdown("## Ukraine Humanitarian Response")
    st.markdown(
        "<p style='color:#7a8fa6'>Live data from Humanitarian Data Exchange (HDX). "
        "Pipeline updates on each run.</p>", unsafe_allow_html=True
    )

    # ── KPI row ───────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    try:
        total_reached = query(engine,
            "SELECT SUM(inter_cluster) FROM dbo.five_w WHERE oblast != 'Grand Total'"
        ).iloc[0, 0]
        col1.metric("People Reached", f"{total_reached/1e6:.1f}M", "Inter-cluster 2025")
    except Exception:
        col1.metric("People Reached", "N/A")

    try:
        food_markets = query(engine,
            "SELECT COUNT(DISTINCT market) FROM dbo.food_prices"
        ).iloc[0, 0]
        col2.metric("Markets Monitored", f"{int(food_markets)}", "WFP Food Prices")
    except Exception:
        col2.metric("Markets Monitored", "N/A")

    try:
        funding_df = query(engine,
            "SELECT SUM(requirements) as req, SUM(funding) as fun FROM dbo.fts_funding"
        )
        gap = (funding_df["req"].iloc[0] - funding_df["fun"].iloc[0]) / 1e9
        col3.metric("Funding Gap", f"${gap:.1f}B", "2014–2026 cumulative")
    except Exception:
        col3.metric("Funding Gap", "N/A")

    try:
        latest_fcs = query(engine,
            """SELECT TOP 1 prevalence, report_date
               FROM dbo.hunger_map
               WHERE admin_level='national' AND indicator='fcs'
               ORDER BY report_date DESC"""
        )
        pct = latest_fcs["prevalence"].iloc[0]
        dt  = str(latest_fcs["report_date"].iloc[0])[:10]
        col4.metric("Food Insecurity", f"{pct:.1%}", f"FCS · {dt}")
    except Exception:
        col4.metric("Food Insecurity", "N/A")

    st.divider()

    # ── Funding trend ─────────────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### Funding vs Requirements by Year")
        try:
            df_fund = query(engine, """
                SELECT report_year,
                       SUM(requirements)/1e6 AS requirements_m,
                       SUM(funding)/1e6      AS funding_m
                FROM dbo.fts_funding
                WHERE report_year >= 2019
                GROUP BY report_year
                ORDER BY report_year
            """)
            fig = go.Figure()
            fig.add_bar(x=df_fund["report_year"], y=df_fund["requirements_m"],
                        name="Requirements", marker_color="#4f8ef7")
            fig.add_bar(x=df_fund["report_year"], y=df_fund["funding_m"],
                        name="Funded", marker_color="#00c896")
            fig.update_layout(**PLOTLY_THEME, barmode="group",
                              xaxis_title="Year", yaxis_title="USD Million",
                              legend=dict(bgcolor="#0e1419"))
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Could not load funding data: {e}")

    with col_b:
        st.markdown("#### Food Insecurity Trend (National FCS)")
        try:
            df_fcs = query(engine, """
                SELECT report_date, prevalence
                FROM dbo.hunger_map
                WHERE admin_level='national' AND indicator='fcs'
                ORDER BY report_date
            """)
            df_fcs["report_date"] = pd.to_datetime(df_fcs["report_date"])
            fig2 = px.line(df_fcs, x="report_date", y="prevalence",
                           labels={"report_date": "Date", "prevalence": "FCS Prevalence"})
            fig2.update_traces(line_color="#f7a14f", line_width=2)
            fig2.update_layout(**PLOTLY_THEME)
            fig2.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig2, use_container_width=True)
        except Exception as e:
            st.error(f"Could not load hunger data: {e}")


# ══════════════════════════════════════════════════════════════
# PAGE: FOOD PRICES
# ══════════════════════════════════════════════════════════════
elif page == "Food Prices":
    st.markdown("## Food Prices — Ukraine Markets")
    st.markdown(
        "<p style='color:#7a8fa6'>WFP food price monitoring across Ukrainian markets. "
        "2014 – present.</p>", unsafe_allow_html=True
    )

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        commodities = query(engine,
            "SELECT DISTINCT commodity FROM dbo.food_prices ORDER BY commodity"
        )["commodity"].tolist()
        selected_commodities = st.multiselect(
            "Commodities",
            commodities,
            default=commodities[:4] if len(commodities) >= 4 else commodities
        )
    with col2:
        year_range = st.slider("Year range", 2014, 2025, (2020, 2025))

    if not selected_commodities:
        st.info("Select at least one commodity.")
        st.stop()

    placeholders = ",".join([f"'{c}'" for c in selected_commodities])
    df_prices = query(engine, f"""
        SELECT report_date, market, commodity, usd_price, latitude, longitude
        FROM dbo.food_prices
        WHERE commodity IN ({placeholders})
          AND YEAR(report_date) BETWEEN {year_range[0]} AND {year_range[1]}
          AND usd_price IS NOT NULL
        ORDER BY report_date
    """)

    if df_prices.empty:
        st.warning("No data for selected filters.")
        st.stop()

    df_prices["report_date"] = pd.to_datetime(df_prices["report_date"])

    # ── Price trend ───────────────────────────────────────────
    st.markdown("#### Price Trends (USD)")
    df_agg = df_prices.groupby(["report_date", "commodity"])["usd_price"].mean().reset_index()
    fig = px.line(df_agg, x="report_date", y="usd_price", color="commodity",
                  labels={"report_date": "Date", "usd_price": "Avg Price (USD)", "commodity": "Commodity"})
    fig.update_layout(**PLOTLY_THEME, legend=dict(bgcolor="#0e1419"))
    st.plotly_chart(fig, use_container_width=True)

    # ── Market map ────────────────────────────────────────────
    st.markdown("#### Market Locations")
    df_map = df_prices.dropna(subset=["latitude", "longitude"])
    df_map = df_map.groupby(["market", "latitude", "longitude"])["usd_price"].mean().reset_index()

    if not df_map.empty:
        # Record count per market for bubble size
        df_counts = df_prices.groupby("market").size().reset_index(name="record_count")
        df_map = df_map.merge(df_counts, on="market", how="left")

        fig_map = px.scatter_mapbox(
            df_map, lat="latitude", lon="longitude",
            hover_name="market",
            hover_data={"usd_price": ":.2f", "record_count": True,
                        "latitude": False, "longitude": False},
            size="record_count",
            size_max=28,
            color="usd_price",
            color_continuous_scale=[
                [0.0, "#00c896"],
                [0.5, "#4f8ef7"],
                [1.0, "#f7a14f"],
            ],
            zoom=5, center={"lat": 49.0, "lon": 32.0},
            mapbox_style="carto-positron",
            labels={"usd_price": "Avg Price (USD)", "record_count": "Data Points"}
        )
        fig_map.update_layout(
            paper_bgcolor="#090d12",
            margin=dict(l=0, r=0, t=0, b=0),
            coloraxis_colorbar=dict(
                title="Avg Price (USD)",
                tickfont=dict(color="#7a8fa6"),
            )
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("No geo coordinates available for selected filters.")


# ══════════════════════════════════════════════════════════════
# PAGE: PEOPLE REACHED (5W)
# ══════════════════════════════════════════════════════════════
elif page == "People Reached (5W)":
    st.markdown("## People Reached by Oblast — 2025")
    st.markdown(
        "<p style='color:#7a8fa6'>5W humanitarian response data. "
        "People reached per cluster per Oblast.</p>", unsafe_allow_html=True
    )

    df_5w = query(engine, """
        SELECT * FROM dbo.five_w
        WHERE oblast != 'Grand Total'
        ORDER BY inter_cluster DESC
    """)

    if df_5w.empty:
        st.warning("No 5W data found.")
        st.stop()

    CLUSTERS = {
        "health":                   "Health",
        "education":                "Education",
        "food_security_livelihoods":"Food Security",
        "protection_general":       "Protection",
        "protection_child":         "Child Protection",
        "protection_gbv":           "GBV",
        "protection_mine_action":   "Mine Action",
        "shelter_nfi":              "Shelter & NFI",
        "wash":                     "WASH",
        "cash_assistance":          "Cash Assistance",
        "inter_cluster":            "Inter-Cluster Total",
    }

    # Cluster selector
    selected_cluster = st.selectbox(
        "Cluster",
        list(CLUSTERS.keys()),
        format_func=lambda x: CLUSTERS[x],
        index=list(CLUSTERS.keys()).index("inter_cluster")
    )

    df_sorted = df_5w[["oblast", selected_cluster]].dropna().sort_values(
        selected_cluster, ascending=True
    )

    col_chart, col_table = st.columns([2, 1])

    with col_chart:
        fig = px.bar(
            df_sorted, x=selected_cluster, y="oblast",
            orientation="h",
            labels={selected_cluster: "People Reached", "oblast": "Oblast"},
            color=selected_cluster,
            color_continuous_scale=[[0, "#1e2d3d"], [1, "#00c896"]],
        )
        fig.update_layout(**PLOTLY_THEME, showlegend=False,
                          coloraxis_showscale=False,
                          xaxis_tickformat=",")
        st.plotly_chart(fig, use_container_width=True)

    with col_table:
        st.markdown(f"#### Top 5 — {CLUSTERS[selected_cluster]}")
        top5 = df_sorted.sort_values(selected_cluster, ascending=False).head(5)
        top5[selected_cluster] = top5[selected_cluster].apply(lambda x: f"{x:,.0f}")
        top5.columns = ["Oblast", "People Reached"]
        st.dataframe(top5, hide_index=True, use_container_width=True)

        total = df_5w[selected_cluster].sum()
        st.metric("Total Reached", f"{total/1e6:.2f}M")

    # ── Cluster comparison heatmap ────────────────────────────
    st.markdown("#### Cluster Comparison Heatmap")
    cluster_cols = [c for c in CLUSTERS.keys() if c != "inter_cluster"]
    df_heat = df_5w[["oblast"] + cluster_cols].set_index("oblast")
    df_heat.columns = [CLUSTERS[c] for c in cluster_cols]

    fig_heat = px.imshow(
        df_heat,
        color_continuous_scale=[[0, "#0e1419"], [0.5, "#4f8ef7"], [1, "#00c896"]],
        labels=dict(color="People Reached"),
        aspect="auto"
    )
    fig_heat.update_layout(**PLOTLY_THEME)
    st.plotly_chart(fig_heat, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# PAGE: FUNDING GAP
# ══════════════════════════════════════════════════════════════
elif page == "Funding Gap":
    st.markdown("## Humanitarian Funding Gap — Ukraine")
    st.markdown(
        "<p style='color:#7a8fa6'>FTS requirements vs actual funding by cluster. "
        "2014–2026.</p>", unsafe_allow_html=True
    )

    years = query(engine,
        "SELECT DISTINCT report_year FROM dbo.fts_funding ORDER BY report_year DESC"
    )["report_year"].tolist()

    selected_year = st.selectbox("Year", years, index=0)

    df_fund = query(engine, f"""
        SELECT cluster,
               SUM(requirements)/1e6 AS requirements_m,
               SUM(funding)/1e6      AS funding_m,
               AVG(pct_funded)       AS pct_funded
        FROM dbo.fts_funding
        WHERE report_year = {selected_year}
          AND cluster IS NOT NULL
        GROUP BY cluster
        ORDER BY requirements_m DESC
    """)

    if df_fund.empty:
        st.warning("No funding data for selected year.")
        st.stop()

    df_fund["gap_m"]     = df_fund["requirements_m"] - df_fund["funding_m"]
    df_fund["pct_funded"]= df_fund["pct_funded"].round(1)

    # ── KPIs ──────────────────────────────────────────────────
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Requirements", f"${df_fund['requirements_m'].sum():,.0f}M")
    col2.metric("Total Funded",        f"${df_fund['funding_m'].sum():,.0f}M")
    gap_total = df_fund["gap_m"].sum()
    col3.metric("Funding Gap",         f"${gap_total:,.0f}M",
                delta=f"-{gap_total/df_fund['requirements_m'].sum()*100:.0f}% unfunded",
                delta_color="inverse")

    st.divider()

    # ── Waterfall chart ───────────────────────────────────────
    st.markdown("#### Requirements vs Funded by Cluster")
    df_top = df_fund.head(12)
    fig = go.Figure()
    fig.add_bar(x=df_top["cluster"], y=df_top["requirements_m"],
                name="Requirements", marker_color="#4f8ef7")
    fig.add_bar(x=df_top["cluster"], y=df_top["funding_m"],
                name="Funded", marker_color="#00c896")
    fig.update_layout(
        **PLOTLY_THEME, barmode="overlay",
        xaxis_tickangle=-35,
        xaxis_title="", yaxis_title="USD Million",
        legend=dict(bgcolor="#0e1419")
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── % funded table ────────────────────────────────────────
    st.markdown("#### Funding Coverage by Cluster")
    df_display = df_fund[["cluster", "requirements_m", "funding_m", "pct_funded", "gap_m"]].copy()
    df_display.columns = ["Cluster", "Required ($M)", "Funded ($M)", "% Funded", "Gap ($M)"]
    df_display["Required ($M)"] = df_display["Required ($M)"].apply(lambda x: f"{x:,.1f}")
    df_display["Funded ($M)"]   = df_display["Funded ($M)"].apply(lambda x: f"{x:,.1f}")
    df_display["Gap ($M)"]      = df_display["Gap ($M)"].apply(lambda x: f"{x:,.1f}")
    df_display["% Funded"]      = df_display["% Funded"].apply(lambda x: f"{x:.1f}%")
    st.dataframe(df_display, hide_index=True, use_container_width=True)
