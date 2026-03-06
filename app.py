"""
Nature's Seed — WooCommerce Sales Dashboard
Reads from cached parquet files produced by fetch_data.py.
"""

import os
import json
from datetime import date, datetime
import calendar

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")

st.set_page_config(page_title="Nature's Seed Dashboard", layout="wide")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def load_meta() -> dict:
    path = os.path.join(CACHE_DIR, "meta.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


@st.cache_data(ttl=3600)
def load_orders(filename: str) -> pd.DataFrame:
    path = os.path.join(CACHE_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


@st.cache_data(ttl=3600)
def load_customer_first_order() -> pd.DataFrame:
    path = os.path.join(CACHE_DIR, "customer_first_order.parquet")
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["first_order_date"] = pd.to_datetime(df["first_order_date"])
    return df


def fmt_currency(v):
    return f"${v:,.0f}"


def fmt_pct(v):
    return f"{v:.1f}%"


def delta_pct(cy, ly):
    if ly == 0:
        return None
    return (cy - ly) / ly * 100


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("Nature's Seed")
meta = load_meta()
page = st.sidebar.selectbox("Navigation", ["MTD View", "YTD Monthly View", "Walmart", "Amazon"])

if meta:
    st.sidebar.caption(f"Last updated: {meta.get('last_updated', 'Unknown')}")
else:
    st.sidebar.caption("No data cached yet.")


# ---------------------------------------------------------------------------
# Check for data
# ---------------------------------------------------------------------------

df_cy = load_orders("orders_cy.parquet")
df_ly = load_orders("orders_ly.parquet")
df_first = load_customer_first_order()

if df_cy.empty and df_ly.empty:
    st.warning("No data cached yet. Run `python fetch_data.py` first.")
    st.stop()


# ---------------------------------------------------------------------------
# Helper: compute KPIs for a filtered DataFrame
# ---------------------------------------------------------------------------

def compute_kpis(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"revenue": 0, "orders": 0, "aov": 0, "gross_margin": 0}
    revenue = df["line_total"].sum()
    orders = df["order_id"].nunique()
    aov = revenue / orders if orders else 0
    gross_margin = df["gross_margin"].sum()
    return {"revenue": revenue, "orders": orders, "aov": aov, "gross_margin": gross_margin}


def render_kpi_row(cy_kpis: dict, ly_kpis: dict):
    cols = st.columns(4)
    labels = [("Revenue", "revenue"), ("Orders", "orders"), ("AOV", "aov"), ("Gross Margin ($)", "gross_margin")]
    for col, (label, key) in zip(cols, labels):
        cy_val = cy_kpis[key]
        ly_val = ly_kpis[key]
        d = cy_val - ly_val
        dp = delta_pct(cy_val, ly_val)
        if key == "orders":
            col.metric(label, f"{cy_val:,.0f}", delta=f"{d:+,.0f} ({fmt_pct(dp) if dp is not None else 'N/A'})")
        else:
            col.metric(label, fmt_currency(cy_val), delta=f"{fmt_currency(d)} ({fmt_pct(dp) if dp is not None else 'N/A'})")


# ---------------------------------------------------------------------------
# PAGE 1: MTD View
# ---------------------------------------------------------------------------

if page == "MTD View":
    st.title("Month-to-Date Performance")

    today = date.today()
    month_options = []
    for m in range(1, today.month + 1):
        month_options.append(date(today.year, m, 1))
    selected_month = st.selectbox(
        "Select month",
        month_options,
        index=len(month_options) - 1,
        format_func=lambda d: d.strftime("%B %Y"),
    )

    sel_year = selected_month.year
    sel_month = selected_month.month
    is_current = sel_month == today.month and sel_year == today.year

    # Date range for CY
    cy_start = datetime(sel_year, sel_month, 1)
    if is_current:
        cy_end = datetime(today.year, today.month, today.day, 23, 59, 59)
    else:
        last_day = calendar.monthrange(sel_year, sel_month)[1]
        cy_end = datetime(sel_year, sel_month, last_day, 23, 59, 59)

    # LY same day range
    ly_start = datetime(sel_year - 1, sel_month, 1)
    if is_current:
        ly_day = min(today.day, calendar.monthrange(sel_year - 1, sel_month)[1])
        ly_end = datetime(sel_year - 1, sel_month, ly_day, 23, 59, 59)
    else:
        last_day_ly = calendar.monthrange(sel_year - 1, sel_month)[1]
        ly_end = datetime(sel_year - 1, sel_month, last_day_ly, 23, 59, 59)

    mtd_cy = df_cy[(df_cy["order_date"] >= cy_start) & (df_cy["order_date"] <= cy_end)]
    mtd_ly = df_ly[(df_ly["order_date"] >= ly_start) & (df_ly["order_date"] <= ly_end)]

    cy_kpis = compute_kpis(mtd_cy)
    ly_kpis = compute_kpis(mtd_ly)
    render_kpi_row(cy_kpis, ly_kpis)

    # --- Daily cumulative revenue chart ---
    st.subheader("Daily Cumulative Revenue")

    def daily_cumulative(df, start, end):
        if df.empty:
            return pd.Series(dtype=float)
        daily = df.groupby(df["order_date"].dt.day)["line_total"].sum()
        max_day = end.day
        idx = range(1, max_day + 1)
        daily = daily.reindex(idx, fill_value=0)
        return daily.cumsum()

    cum_cy = daily_cumulative(mtd_cy, cy_start, cy_end)
    cum_ly = daily_cumulative(mtd_ly, ly_start, ly_end)

    fig = go.Figure()
    if not cum_cy.empty:
        fig.add_trace(go.Scatter(x=list(cum_cy.index), y=cum_cy.values, name="This Year", line=dict(color="#2E7D32")))
    if not cum_ly.empty:
        fig.add_trace(go.Scatter(x=list(cum_ly.index), y=cum_ly.values, name="Last Year", line=dict(color="#9E9E9E", dash="dash")))
    fig.update_layout(xaxis_title="Day of Month", yaxis_title="Cumulative Revenue ($)", height=400, margin=dict(t=20))
    st.plotly_chart(fig, use_container_width=True)

    # --- Tabs ---
    tab_cat, tab_cust = st.tabs(["By Product Category", "By Customer Type"])

    with tab_cat:
        cat_cy = mtd_cy.groupby("category")["line_total"].sum().reset_index().rename(columns={"line_total": "CY Revenue"})
        cat_ly = mtd_ly.groupby("category")["line_total"].sum().reset_index().rename(columns={"line_total": "LY Revenue"})
        cat_merged = pd.merge(cat_cy, cat_ly, on="category", how="outer").fillna(0)
        cat_merged["Change $"] = cat_merged["CY Revenue"] - cat_merged["LY Revenue"]
        cat_merged["Change %"] = cat_merged.apply(lambda r: delta_pct(r["CY Revenue"], r["LY Revenue"]), axis=1)
        cat_merged = cat_merged.sort_values("CY Revenue", ascending=False)

        fig_cat = go.Figure()
        fig_cat.add_trace(go.Bar(x=cat_merged["category"], y=cat_merged["CY Revenue"], name="CY", marker_color="#2E7D32"))
        fig_cat.add_trace(go.Bar(x=cat_merged["category"], y=cat_merged["LY Revenue"], name="LY", marker_color="#9E9E9E"))
        fig_cat.update_layout(barmode="group", height=400, margin=dict(t=20), yaxis_title="Revenue ($)")
        st.plotly_chart(fig_cat, use_container_width=True)

        display_cat = cat_merged.copy()
        display_cat["CY Revenue"] = display_cat["CY Revenue"].apply(fmt_currency)
        display_cat["LY Revenue"] = display_cat["LY Revenue"].apply(fmt_currency)
        display_cat["Change $"] = display_cat["Change $"].apply(fmt_currency)
        display_cat["Change %"] = display_cat["Change %"].apply(lambda v: fmt_pct(v) if v is not None else "N/A")
        display_cat.columns = ["Category", "CY Revenue", "LY Revenue", "Change $", "Change %"]
        st.dataframe(display_cat, use_container_width=True, hide_index=True)

    with tab_cust:
        if df_first.empty:
            st.info("Customer first-order data not available.")
        else:
            def classify_customers(df, period_start, period_end):
                if df.empty:
                    return pd.DataFrame(), pd.DataFrame()
                merged = df.merge(df_first, on="customer_id", how="left")
                merged["is_new"] = (
                    (merged["first_order_date"].dt.year == period_start.year)
                    & (merged["first_order_date"].dt.month == period_start.month)
                )
                new = merged[merged["is_new"]]
                returning = merged[~merged["is_new"]]
                return new, returning

            new_cy, ret_cy = classify_customers(mtd_cy, cy_start, cy_end)
            new_ly, ret_ly = classify_customers(mtd_ly, ly_start, ly_end)

            st.markdown("**New Customers**")
            render_kpi_row(compute_kpis(new_cy), compute_kpis(new_ly))

            st.markdown("**Returning Customers**")
            render_kpi_row(compute_kpis(ret_cy), compute_kpis(ret_ly))

            # Grouped bar: New vs Returning
            labels = ["New", "Returning"]
            cy_vals = [new_cy["line_total"].sum() if not new_cy.empty else 0,
                       ret_cy["line_total"].sum() if not ret_cy.empty else 0]
            ly_vals = [new_ly["line_total"].sum() if not new_ly.empty else 0,
                       ret_ly["line_total"].sum() if not ret_ly.empty else 0]

            fig_cust = go.Figure()
            fig_cust.add_trace(go.Bar(x=labels, y=cy_vals, name="CY", marker_color="#2E7D32"))
            fig_cust.add_trace(go.Bar(x=labels, y=ly_vals, name="LY", marker_color="#9E9E9E"))
            fig_cust.update_layout(barmode="group", height=350, margin=dict(t=20), yaxis_title="Revenue ($)")
            st.plotly_chart(fig_cust, use_container_width=True)


# ---------------------------------------------------------------------------
# PAGE 2: YTD Monthly View
# ---------------------------------------------------------------------------

elif page == "YTD Monthly View":
    st.title("Year-to-Date Monthly Comparison")

    today = date.today()

    def monthly_summary(df, year, max_month, is_cy=False):
        rows = []
        for m in range(1, max_month + 1):
            start = datetime(year, m, 1)
            if is_cy and m == today.month:
                end = datetime(year, m, today.day, 23, 59, 59)
            else:
                last_day = calendar.monthrange(year, m)[1]
                end = datetime(year, m, last_day, 23, 59, 59)
            mdf = df[(df["order_date"] >= start) & (df["order_date"] <= end)]
            kpis = compute_kpis(mdf)
            kpis["month"] = m
            rows.append(kpis)
        return pd.DataFrame(rows)

    def monthly_summary_ly_matched(df, max_month, today_obj):
        """LY monthly summary with the current month matched to same day range."""
        rows = []
        for m in range(1, max_month + 1):
            start = datetime(today_obj.year - 1, m, 1)
            if m == today_obj.month:
                ly_day = min(today_obj.day, calendar.monthrange(today_obj.year - 1, m)[1])
                end = datetime(today_obj.year - 1, m, ly_day, 23, 59, 59)
            else:
                last_day = calendar.monthrange(today_obj.year - 1, m)[1]
                end = datetime(today_obj.year - 1, m, last_day, 23, 59, 59)
            mdf = df[(df["order_date"] >= start) & (df["order_date"] <= end)]
            kpis = compute_kpis(mdf)
            kpis["month"] = m
            rows.append(kpis)
        return pd.DataFrame(rows)

    cy_monthly = monthly_summary(df_cy, today.year, today.month, is_cy=True)
    ly_monthly = monthly_summary_ly_matched(df_ly, today.month, today)

    # Also get full LY for trend chart (all 12 months)
    ly_full = monthly_summary(df_ly, today.year - 1, 12)

    # Build display table
    table_rows = []
    for _, cy_row in cy_monthly.iterrows():
        m = int(cy_row["month"])
        ly_row = ly_monthly[ly_monthly["month"] == m]
        ly_kpis = ly_row.iloc[0] if not ly_row.empty else {"revenue": 0, "orders": 0, "aov": 0, "gross_margin": 0}

        month_label = calendar.month_name[m]
        if m == today.month:
            month_label += " (MTD)"

        table_rows.append({
            "Month": month_label,
            "CY Revenue": cy_row["revenue"],
            "LY Revenue": ly_kpis["revenue"] if isinstance(ly_kpis, pd.Series) else ly_kpis.get("revenue", 0),
            "Rev %": delta_pct(cy_row["revenue"], ly_kpis["revenue"] if isinstance(ly_kpis, pd.Series) else 0),
            "CY Orders": cy_row["orders"],
            "LY Orders": ly_kpis["orders"] if isinstance(ly_kpis, pd.Series) else 0,
            "Orders %": delta_pct(cy_row["orders"], ly_kpis["orders"] if isinstance(ly_kpis, pd.Series) else 0),
            "CY AOV": cy_row["aov"],
            "LY AOV": ly_kpis["aov"] if isinstance(ly_kpis, pd.Series) else 0,
            "AOV %": delta_pct(cy_row["aov"], ly_kpis["aov"] if isinstance(ly_kpis, pd.Series) else 0),
            "CY Gross Margin": cy_row["gross_margin"],
            "LY Gross Margin": ly_kpis["gross_margin"] if isinstance(ly_kpis, pd.Series) else 0,
            "GM %": delta_pct(cy_row["gross_margin"], ly_kpis["gross_margin"] if isinstance(ly_kpis, pd.Series) else 0),
        })

    table_df = pd.DataFrame(table_rows)

    def color_delta(val):
        if val is None or pd.isna(val):
            return ""
        if val > 0:
            return "color: #2E7D32; background-color: #E8F5E9"
        elif val < 0:
            return "color: #C62828; background-color: #FFEBEE"
        return ""

    # Format for display
    styled_df = table_df.copy()
    delta_cols = ["Rev %", "Orders %", "AOV %", "GM %"]
    currency_cols = ["CY Revenue", "LY Revenue", "CY AOV", "LY AOV", "CY Gross Margin", "LY Gross Margin"]
    order_cols = ["CY Orders", "LY Orders"]

    styler = styled_df.style
    styler = styler.format({
        **{c: "${:,.0f}" for c in currency_cols},
        **{c: "{:,.0f}" for c in order_cols},
        **{c: lambda v: fmt_pct(v) if v is not None and not pd.isna(v) else "N/A" for c in delta_cols},
    })
    styler = styler.applymap(color_delta, subset=delta_cols)

    st.subheader("Monthly Summary")
    st.dataframe(styler, use_container_width=True, hide_index=True)

    # --- Monthly revenue bar chart ---
    st.subheader("Monthly Revenue Comparison")
    month_names = [calendar.month_abbr[int(m)] for m in cy_monthly["month"]]
    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(x=month_names, y=cy_monthly["revenue"], name=f"{today.year}", marker_color="#2E7D32"))
    ly_rev_matched = [ly_monthly[ly_monthly["month"] == m]["revenue"].values[0] if not ly_monthly[ly_monthly["month"] == m].empty else 0 for m in cy_monthly["month"]]
    fig_bar.add_trace(go.Bar(x=month_names, y=ly_rev_matched, name=f"{today.year - 1}", marker_color="#9E9E9E"))
    fig_bar.update_layout(barmode="group", height=400, margin=dict(t=20), yaxis_title="Revenue ($)")
    st.plotly_chart(fig_bar, use_container_width=True)

    # --- Monthly trend line chart ---
    st.subheader("Monthly Revenue Trend")
    fig_line = go.Figure()
    cy_months = [calendar.month_abbr[int(m)] for m in cy_monthly["month"]]
    ly_months_full = [calendar.month_abbr[int(m)] for m in ly_full["month"]]
    fig_line.add_trace(go.Scatter(x=cy_months, y=cy_monthly["revenue"].values, name=f"{today.year}", line=dict(color="#2E7D32")))
    fig_line.add_trace(go.Scatter(x=ly_months_full, y=ly_full["revenue"].values, name=f"{today.year - 1}", line=dict(color="#9E9E9E", dash="dash")))
    fig_line.update_layout(height=400, margin=dict(t=20), yaxis_title="Revenue ($)", xaxis_title="Month")
    st.plotly_chart(fig_line, use_container_width=True)


# ---------------------------------------------------------------------------
# PAGE 3: Walmart (Placeholder)
# ---------------------------------------------------------------------------

elif page == "Walmart":
    st.title("Walmart Marketplace")
    st.caption("Walmart Seller API integration coming soon. This tab will show MTD and monthly sales data from Walmart Marketplace alongside WooCommerce.")

    cols = st.columns(3)
    for col, label in zip(cols, ["Revenue", "Orders", "AOV"]):
        col.metric(label, "---", delta=None)

    st.info("To enable: add `WALMART_CLIENT_ID` and `WALMART_CLIENT_SECRET` to environment variables.")


# ---------------------------------------------------------------------------
# PAGE 4: Amazon (Placeholder)
# ---------------------------------------------------------------------------

elif page == "Amazon":
    st.title("Amazon Seller Central")
    st.caption("Amazon SP-API integration coming soon. This tab will show MTD and monthly sales data from Amazon alongside WooCommerce.")

    cols = st.columns(3)
    for col, label in zip(cols, ["Revenue", "Orders", "AOV"]):
        col.metric(label, "---", delta=None)

    st.info("To enable: add `AMAZON_SELLER_ID`, `AMAZON_MWS_ACCESS_KEY`, and `AMAZON_MWS_SECRET_KEY` to environment variables.")
