# AgriSight: Agricultural Commodity Price Analysis & Dashboarding

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/Python-3.8+-green.svg)
![Azure](https://img.shields.io/badge/Azure-Cloud%20Native-0078D4.svg)

## Overview

AgriSight is a data analytics portfolio project that explores US agricultural commodity sales (2019–2023) alongside global FAO production data to uncover price trends, regional patterns, and category-level performance.

The project covers an end-to-end analytics workflow: data ingestion, cleaning, enrichment, feature engineering, and interactive Tableau dashboards. It is structured around the **medallion architecture** pattern (Bronze → Silver → Gold) to keep raw, cleaned, and analytical data clearly separated.

**Data sources:**
- [US Agricultural Sales Dataset (2019–2023)](https://www.kaggle.com/datasets/mikeeddie/us-agricultural-sales-dataset-2019-2023) — USDA/Kaggle
- [FAOSTAT Crops & Livestock Data](https://www.kaggle.com/datasets/vijayveersingh/faostat-crops-and-livestock-data) — FAO via Kaggle

---

## What I Built

- - A multi-source data pipeline in Azure Data Factory to ingest CSV files from GitHub, a MySQL source, and Azure SQL Database.
- Bronze → Silver → Gold data layers using Azure Data Lake Storage Gen2 and Databricks (Apache Spark).
- Fuzzy string matching to join US sales records with FAO global production data — 68.2% of records (1,125 of 1,650) were successfully matched.
- 46 engineered features covering price, volume, production efficiency, and financial metrics
- Correlation analysis to identify the strongest price drivers.
- A Tableau dashboard with KPI cards, time-series trends, geographic mapping, and category breakdowns.

---

## Technology Stack

| Layer | Tools |
|-------|-------|
| Orchestration | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2, Azure Blob Storage |
| Processing | Azure Databricks, Apache Spark, Python |
| Enrichment Lookup | MongoDB |
| Source Systems | MySQL, Azure SQL Database |
| Visualization | Tableau |
| Languages | Python, SQL |

---

## Pipeline Architecture

```
DATA SOURCES
  GitHub CSVs │ MySQL Database │ Azure SQL Database
                        │
                        ▼
        AZURE DATA FACTORY (Orchestration)
          Pipeline 1: US Sales Ingestion
          Pipeline 2: FAOSTAT Ingestion
                        │
                        ▼
        BRONZE LAYER — Raw data (ADLS Gen2)
                        │
                        ▼
        SILVER LAYER — Cleaned & enriched
          Databricks + Spark + MongoDB
          - Type casting & unit standardization
          - Fuzzy string matching (US ↔ FAO)
          - Data quality validation
                        │
                        ▼
        GOLD LAYER — Analytical tables
          46 engineered features
                        │
                        ▼
        TABLEAU DASHBOARDS
```

---

## Key Findings

| Metric | Value | Notes |
|--------|-------|-------|
| Total sales volume | ~64.4M lbs | 5-year aggregate across 22 commodities |
| Average sale price | $7.99/lb | Across all products and states |
| Price inertia | r = 0.999 | Rolling 2-year average is the dominant price predictor |
| Production efficiency | r = 0.24 | Secondary price driver |
| Demand-supply index | r = −0.25 | Oversupply correlates with lower prices |
| FAO enrichment match rate | 68.2% | 1,125 of 1,650 records matched |

The strongest price predictor was the **rolling 2-year average price** (r = 0.999), suggesting agricultural commodity prices are highly inertial. Recent history is the best guide to near-term prices.

### Top 10 Products by Volume

1. Apples — 3.5M lbs
2. Bell Peppers — 3.1M lbs
3. Rosemary — 2.8M lbs
4. Grapes — 2.8M lbs
5. Parsley — 2.4M lbs
6. Tomatoes — 2.2M lbs
7. Peaches — 2.2M lbs
8. Spinach — 1.9M lbs
9. Tarragon — 1.7M lbs
10. Watermelons — 1.6M lbs

![Top 10 Products by Volume](./Images/Top_10.png)

### Geographic Distribution

- **California**: ~15.2M lbs (24% of US total)
- **Texas**: ~8.9M lbs (14%)
- **Florida**: ~7.1M lbs (11%)
- **New York**: ~6.3M lbs (10%)
- **Washington**: ~5.4M lbs (8%)

The top 5 states account for roughly 67% of national sales volume — a significant geographic concentration.

### 5-Year Sales Trend (2019–2023)

- 2019–2020: Stable baseline (~13.5M lbs/year).
- 2021: Dip (~12M lbs) — likely supply chain effects.
- 2022–2023: Partial recovery (~11.5M lbs).

Overall volume declined ~21% over the period, with fruits showing the most resilience across categories.

---

## Dashboard

The Tableau dashboard includes:

- **KPI Cards**: Total volume, average price, average profit margin, YoY variance.
- **Category Pie Chart**: Fruits (34.6%), Vegetables (33.1%), Herbs (32.1%).
- **Bar Chart**: Top 10 products by sales volume.
- **Time Series**: Annual sales trend 2019–2023.
- **Choropleth Map**: State-level volume distribution.

![AgriSight Dashboard](./Images/dashboard.png)

---

## Engineered Features (Selected)

| Feature | Description |
|---------|-------------|
| `avg_sale_price` | Mean price per product, year, and state |
| `price_yoy_change` | Year-over-year price change (%) |
| `rolling_avg_price_2y` | 2-year rolling average for trend smoothing |
| `price_volatility_index` | Standard deviation of price over time |
| `total_quantity_sold_lb` | Aggregated sales volume in pounds |
| `production_yoy_growth` | Annual production growth rate |
| `demand_supply_index` | Ratio of sales to production (market balance, 0–1) |
| `production_per_area` | Yield efficiency (tons per hectare) |
| `profit_margin_pct` | Gross margin percentage |

Full feature definitions are in [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md).

---

## Data Quality

| Subset | Completeness |
|--------|-------------|
| Matched records (1,125) | 99.8% |
| Unmatched records (525) | 98.5% |

Three features had meaningful outliers: `price_yoy_change` (1.1% of records) and `production_yoy_growth` (3.6%). These were retained as valid — they reflect real market events like crop failures and bumper harvests, not data errors.

![Outlier Detection](./Images/Outlier_detection.png)

---

## Limitations

- The FAO enrichment match rate (68.2%) means 31.8% of records lack global production context, which may affect analysis of less common commodities.
- Forecasting models (ARIMA, ML) are not yet implemented — current analysis is descriptive and correlational.
- The dataset covers 2019–2023 only; longer time horizons would strengthen trend conclusions.
- Profit margin estimates rely on assumptions baked into the source dataset, not independently verified cost data.

---

## Project Structure

```
agrisight/
├── README.md
├── notebooks/
│   └── AgriSight.ipynb              # Main pipeline notebook
├── databricks/
│   └── data_overboard.ipynb         # Cleaning and analysis notebook
├── sql/
│   ├── area_codes.sql
│   ├── production.sql
│   ├── elements.sql
│   └── flags.sql
├── pipeline/
│   ├── FAOSTAT.json                 # ADF pipeline definition
│   └── US_data.json                 # ADF pipeline definition
├── data/
│   ├── bronze/                      # Raw ingested data
│   ├── silver/                      # Cleaned and enriched
│   └── gold/                        # Business-level aggregates
└── docs/
    ├── ARCHITECTURE.md
    └── DATA_DICTIONARY.md
```

---

## What I Would Do Next

- Add ML forecasting models (ARIMA, Prophet) for price prediction.
- Integrate weather and climate data as additional price drivers.
- - Improve the FAO enrichment match rate with better normalization and matching rules.
- Automate anomaly detection on price and volume metrics.
- Expand to additional crop categories and international markets.

---

## Acknowledgments

- American International University-Bangladesh (AIUB) for computational resources.
- USDA & Kaggle for agricultural sales data.
- Food and Agriculture Organization (FAO) for FAOSTAT production metrics.
- Microsoft Azure, Databricks, and Tableau communities for documentation and support.

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

**Last Updated**: June 2026 · **Maintainer**: Shuvro Sankar Sen
