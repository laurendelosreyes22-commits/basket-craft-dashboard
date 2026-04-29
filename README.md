# BasketCraft Dashboard

**Live app:** https://laurendelosreyes22-commits-basket-craft-dashboard-app-x3mjwg.streamlit.app/

A Streamlit dashboard connected to Snowflake that analyzes BasketCraft e-commerce data.

## Features

- **Headline Metrics** — Total revenue, orders, average order value, and items sold for the most recent month, each with a month-over-month delta
- **Revenue Trend** — Monthly revenue line chart with relative date presets (Last 3/6/12 months, Year to date, All time) and manual date pickers
- **Top Products by Revenue** — Horizontal bar chart ranked by revenue, respects the date filter
- **Bundle Finder** — Select a product and see which other products appear most often in the same orders, with a CSV download

## Setup

1. Clone the repo and install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file in the project root with your Snowflake credentials:
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_user
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_WAREHOUSE=your_warehouse
   SNOWFLAKE_DATABASE=your_database
   SNOWFLAKE_SCHEMA=your_schema
   ```

3. Run the app:
   ```bash
   streamlit run app.py
   ```

## Deployment

Deployed on [Streamlit Community Cloud](https://streamlit.io/cloud). Snowflake credentials are stored as Streamlit secrets (not committed to the repo).
