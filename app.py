import os
from datetime import date
from dateutil.relativedelta import relativedelta

import altair as alt
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

# Load credentials from .env locally; Streamlit Cloud uses st.secrets instead
load_dotenv()


def _secret(key: str) -> str:
    # Prefer st.secrets (Streamlit Cloud) over environment variables (local .env)
    return st.secrets.get(key) or os.environ[key]


# --- Database connection ---

@st.cache_resource
def get_connection():
    # Reuse a single connection across reruns
    return snowflake.connector.connect(
        account=_secret("SNOWFLAKE_ACCOUNT"),
        user=_secret("SNOWFLAKE_USER"),
        password=_secret("SNOWFLAKE_PASSWORD"),
        role=_secret("SNOWFLAKE_ROLE"),
        warehouse=_secret("SNOWFLAKE_WAREHOUSE"),
        database=_secret("SNOWFLAKE_DATABASE"),
        schema=_secret("SNOWFLAKE_SCHEMA"),
    )


# --- Data queries ---


@st.cache_data(ttl=600)
def get_headline_metrics():
    # Computes the most recent month's KPIs alongside the prior month's for delta calculation.
    # CREATED_AT is stored as a nanosecond Unix timestamp, so scale=9 converts it to a timestamp.
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH monthly AS (
                SELECT
                    DATE_TRUNC('month', TO_TIMESTAMP(CREATED_AT, 9)) AS month,
                    SUM(PRICE_USD)                                    AS revenue,
                    COUNT(DISTINCT ORDER_ID)                          AS orders,
                    SUM(ITEMS_PURCHASED)                              AS items_sold
                FROM RAW.ORDERS
                GROUP BY 1
            ),
            current_month AS (
                SELECT * FROM monthly ORDER BY month DESC LIMIT 1
            ),
            prior_month AS (
                -- OFFSET 1 gives the second-most-recent month
                SELECT * FROM monthly ORDER BY month DESC LIMIT 1 OFFSET 1
            )
            SELECT
                c.revenue,
                c.orders,
                c.items_sold,
                c.revenue / NULLIF(c.orders, 0)         AS avg_order_value,
                p.revenue                               AS prev_revenue,
                p.orders                                AS prev_orders,
                p.items_sold                            AS prev_items_sold,
                p.revenue / NULLIF(p.orders, 0)         AS prev_avg_order_value
            FROM current_month c, prior_month p
            """
        )
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        return None


@st.cache_data(ttl=600)
def get_revenue_trend(start_date: date, end_date: date) -> pd.DataFrame:
    # Returns monthly revenue totals within the selected date range.
    # Cached per unique (start_date, end_date) pair so filter changes re-query efficiently.
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                DATE_TRUNC('month', TO_TIMESTAMP(CREATED_AT, 9))::DATE AS month,
                SUM(PRICE_USD)                                          AS revenue
            FROM RAW.ORDERS
            WHERE TO_TIMESTAMP(CREATED_AT, 9)::DATE BETWEEN %(start)s AND %(end)s
            GROUP BY 1
            ORDER BY 1
            """,
            {"start": start_date, "end": end_date},
        )
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["Month", "Revenue"])
    df["Month"] = pd.to_datetime(df["Month"])
    return df


@st.cache_data(ttl=600)
def get_product_names() -> list[str]:
    # Returns all product names for the bundle finder selector.
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT PRODUCT_NAME FROM RAW.PRODUCTS ORDER BY PRODUCT_NAME")
        return [row[0] for row in cur.fetchall()]


@st.cache_data(ttl=600)
def get_bundle_pairs(product_name: str) -> pd.DataFrame:
    # Finds products most often bought in the same order as the selected product.
    # Looks up orders containing that product, then counts how many of those orders
    # also contain each other product in the catalog.
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH target_orders AS (
                -- All orders that include the chosen product
                SELECT DISTINCT oi.ORDER_ID
                FROM RAW.ORDER_ITEMS oi
                JOIN RAW.PRODUCTS p ON p.PRODUCT_ID = oi.PRODUCT_ID
                WHERE p.PRODUCT_NAME = %(product)s
            )
            SELECT
                p.PRODUCT_NAME                      AS paired_product,
                COUNT(DISTINCT oi.ORDER_ID)         AS orders_together
            FROM RAW.ORDER_ITEMS oi
            JOIN RAW.PRODUCTS p ON p.PRODUCT_ID = oi.PRODUCT_ID
            WHERE oi.ORDER_ID IN (SELECT ORDER_ID FROM target_orders)
              AND p.PRODUCT_NAME != %(product)s
            GROUP BY 1
            ORDER BY 2 DESC
            """,
            {"product": product_name},
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["Paired Product", "Orders Together"])


# --- Helpers ---

@st.cache_data(ttl=600)
def get_top_products(start_date: date, end_date: date) -> pd.DataFrame:
    # Joins ORDER_ITEMS to PRODUCTS to get revenue by product name within the date range.
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                p.PRODUCT_NAME,
                SUM(oi.PRICE_USD) AS revenue
            FROM RAW.ORDER_ITEMS oi
            JOIN RAW.PRODUCTS p ON p.PRODUCT_ID = oi.PRODUCT_ID
            WHERE TO_TIMESTAMP(oi.CREATED_AT, 9)::DATE BETWEEN %(start)s AND %(end)s
            GROUP BY 1
            ORDER BY 2 DESC
            """,
            {"start": start_date, "end": end_date},
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["Product", "Revenue"])


def pct_delta(current, previous):
    """Return a formatted percentage delta string for st.metric."""
    if previous and previous != 0:
        return f"{((current - previous) / previous * 100):+.1f}%"
    return None


# --- Dashboard layout ---

st.title("BasketCraft Dashboard")

# Headline KPI tiles — most recent month vs. prior month
st.subheader("Headline Metrics")
try:
    metrics = get_headline_metrics()
    if metrics:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(
            "Total Revenue",
            f"${metrics['REVENUE']:,.2f}",
            pct_delta(metrics['REVENUE'], metrics['PREV_REVENUE']),
        )
        col2.metric(
            "Total Orders",
            f"{int(metrics['ORDERS']):,}",
            pct_delta(metrics['ORDERS'], metrics['PREV_ORDERS']),
        )
        col3.metric(
            "Avg Order Value",
            f"${metrics['AVG_ORDER_VALUE']:,.2f}",
            pct_delta(metrics['AVG_ORDER_VALUE'], metrics['PREV_AVG_ORDER_VALUE']),
        )
        col4.metric(
            "Total Items Sold",
            f"{int(metrics['ITEMS_SOLD']):,}",
            pct_delta(metrics['ITEMS_SOLD'], metrics['PREV_ITEMS_SOLD']),
        )
    else:
        st.info("No order data found.")
except Exception as e:
    st.warning(f"Could not load headline metrics: {e}")

# --- Sidebar date filter (applies to Revenue Trend and Top Products) ---

DATA_END = date(2026, 3, 19)   # last date present in the dataset
DATA_START = date(2023, 3, 1)  # first date present in the dataset

RANGE_OPTIONS = {
    "Last 3 months":  DATA_END - relativedelta(months=3),
    "Last 6 months":  DATA_END - relativedelta(months=6),
    "Last 12 months": DATA_END - relativedelta(months=12),
    "Year to date":   DATA_END.replace(month=1, day=1),
    "All time":       DATA_START,
}

# Filter column (narrow) beside the two charts (wide)
filter_col, chart_col = st.columns([1, 3])

with filter_col:
    st.subheader("Date Filter")
    # Preset dropdown sets the default for the date pickers.
    # Keying the pickers on the selected range resets them whenever the preset changes,
    # while still letting the user fine-tune the exact dates manually.
    selected_range = st.selectbox("Quick select", list(RANGE_OPTIONS.keys()), index=2)
    preset_start = RANGE_OPTIONS[selected_range]

    start_date = st.date_input(
        "From",
        value=preset_start,
        min_value=DATA_START,
        max_value=DATA_END,
        key=f"start_{selected_range}",
    )
    end_date = st.date_input(
        "To",
        value=DATA_END,
        min_value=DATA_START,
        max_value=DATA_END,
        key=f"end_{selected_range}",
    )
    if start_date > end_date:
        st.warning("'From' must be on or before 'To'.")

with chart_col:
    # Revenue trend line chart
    st.subheader("Revenue Trend")
    if start_date <= end_date:
        try:
            trend_df = get_revenue_trend(start_date, end_date)
            if not trend_df.empty:
                chart = (
                    alt.Chart(trend_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("Month:T", title="Month", axis=alt.Axis(format="%b %Y")),
                        y=alt.Y("Revenue:Q", title="Revenue (USD)", axis=alt.Axis(format="$,.0f")),
                        tooltip=[
                            alt.Tooltip("Month:T", title="Month", format="%B %Y"),
                            alt.Tooltip("Revenue:Q", title="Revenue", format="$,.2f"),
                        ],
                    )
                    .properties(height=300)
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No revenue data for the selected date range.")
        except Exception as e:
            st.warning(f"Could not load revenue trend: {e}")

    # Top products bar chart
    st.subheader("Top Products by Revenue")
    if start_date <= end_date:
        try:
            products_df = get_top_products(start_date, end_date)
            if not products_df.empty:
                bar_chart = (
                    alt.Chart(products_df)
                    .mark_bar()
                    .encode(
                        x=alt.X("Revenue:Q", title="Revenue (USD)", axis=alt.Axis(format="$,.0f")),
                        y=alt.Y("Product:N", sort="-x", title=None),
                        tooltip=[
                            alt.Tooltip("Product:N", title="Product"),
                            alt.Tooltip("Revenue:Q", title="Revenue", format="$,.2f"),
                        ],
                    )
                    .properties(height=40 * len(products_df) + 50)
                )
                st.altair_chart(bar_chart, use_container_width=True)
            else:
                st.info("No product data for the selected date range.")
        except Exception as e:
            st.warning(f"Could not load top products: {e}")

# Bundle finder — pick a product, see what gets bought with it most often
st.subheader("Bundle Finder")
try:
    product_names = get_product_names()
    selected_product = st.selectbox("Select a product", product_names)

    bundle_df = get_bundle_pairs(selected_product)
    if not bundle_df.empty:
        st.dataframe(bundle_df, use_container_width=True, hide_index=True)
        st.download_button(
            label="Download as CSV",
            data=bundle_df.to_csv(index=False),
            file_name=f"bundles_{selected_product.replace(' ', '_')}.csv",
            mime="text/csv",
        )
    else:
        st.info("No co-purchase data found for this product.")
except Exception as e:
    st.warning(f"Could not load bundle finder: {e}")

