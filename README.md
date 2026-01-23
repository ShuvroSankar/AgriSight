# AgriSight: Cloud-Native Agricultural Commodity Price Analytics & Forecasting

![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/Python-3.8+-green.svg)
![Azure](https://img.shields.io/badge/Azure-Cloud%20Native-0078D4.svg)

## Overview

**AgriSight** is a production-ready cloud-native analytics platform that integrates multi-source agricultural data for comprehensive price analysis and trend forecasting. The platform processes **64.4 million pounds** of US agricultural sales data (2019-2023) alongside FAO global production metrics, achieving **68.2% automated data enrichment** through intelligent matching algorithms.

This project demonstrates enterprise-grade data engineering practices using modern cloud technologies, implementing the **medallion architecture** pattern for scalable, governed data pipelines.

---

## Key Features

### Data Integration
- **Multi-Source Pipeline**: Seamlessly ingests data from GitHub, MySQL, and Azure SQL Database
- **Incremental Loading**: Watermark-based change data capture for efficient updates
- **Automated Enrichment**: 68.2% match rate between US sales and global FAOSTAT production metrics
- **Quality Governance**: Three-layer medallion architecture (Bronze → Silver → Gold)

### Analytics Capabilities
- **46 Engineered Features**: Price metrics, volume indicators, production efficiency, financial ratios
- **Correlation Analysis**: Identifies price drivers (rolling 2-year average: r=0.999)
- **Geographic Analysis**: State-level performance benchmarking across 50 US states
- **Temporal Trends**: 5-year historical analysis (2019-2023)

### Visualization & BI
- **Interactive Dashboards**: Tableau-powered drill-down analysis
- **KPI Monitoring**: Real-time summary cards and trend indicators
- **Geographic Mapping**: State-level volume distribution visualization
- **Category Breakdown**: Fruit, vegetable, and herb analysis

---

## Key Findings

| Metric | Value | Insight |
|--------|-------|---------|
| **Price Inertia** | r = 0.999 | Rolling 2-year averages dominate price prediction |
| **Production Efficiency** | r = 0.24 | Secondary but measurable price driver |
| **Demand-Supply Index** | r = -0.25 | Market oversupply depresses prices |
| **Data Enrichment Rate** | 68.2% | 1,125 of 1,650 records matched to FAOSTAT |
| **Total Sales Volume** | 64.4M lbs | 5-year aggregated US agricultural sales |
| **Average Sale Price** | $7.99/lb | Across 22 commodity types |

### Top Performing Products (by volume)
1. 🍎 Apples: 3.5M lbs
2. 🫑 Bell Peppers: 3.1M lbs
3. 🌿 Rosemary: 2.8M lbs
4. 🍇 Grapes: 2.8M lbs
5. 🌱 Parsley: 2.4M lbs

### Market Leaders (by geography)
- **California**: 15.2M lbs (24% of US total)
- **Texas**: 8.9M lbs (14%)
- **Florida**: 7.1M lbs (11%)
- **New York**: 6.3M lbs (10%)
- **Washington**: 5.4M lbs (8%)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│  GitHub CSVs  │  MySQL Database  │  Azure SQL Database          │
└────────────────────────┬──────────────────────────────────────────┘
                         │
                         ▼
        ┌─────────────────────────────────────┐
        │   AZURE DATA FACTORY ORCHESTRATION   │
        │  (Pipeline 1: US Data Ingestion)    │
        │  (Pipeline 2: FAOSTAT Ingestion)    │
        └────────────┬────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────────┐
        │  BRONZE LAYER (Raw Data Lake)       │
        │  Azure Blob Storage + ADLS Gen2     │
        └────────────┬────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────────┐
        │  SILVER LAYER (Cleaned & Enriched)  │
        │  Databricks Apache Spark + MongoDB  │
        │  - Type Casting                     │
        │  - Unit Standardization             │
        │  - Fuzzy String Matching            │
        │  - Data Quality Validation          │
        └────────────┬────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────────┐
        │   GOLD LAYER (Business Aggregates)  │
        │  Analytical Tables & Engineered     │
        │  Features (46 metrics)              │
        └────────────┬────────────────────────┘
                     │
                     ▼
        ┌─────────────────────────────────────┐
        │   TABLEAU DASHBOARDS & ANALYTICS    │
        │   Interactive Visualization         │
        │   Drill-Down Analysis               │
        └─────────────────────────────────────┘
```

---

## Technology Stack

### Cloud Infrastructure
- **Microsoft Azure**: Cloud platform
  - Azure Data Factory (ETL orchestration)
  - Azure Data Lake Storage Gen2 (ADLS)
  - Azure Blob Storage
  - Azure SQL Database
  - Azure Databricks

### Data Processing
- **Apache Spark**: Distributed data processing
- **Python**: Data transformation and UDFs
- **Databricks**: Lakehouse platform with notebook environment

### Data Storage & Integration
- **MongoDB**: Metadata and enrichment lookup
- **MySQL**: Legacy source system
- **ADLS Gen2**: Primary data lake storage

### Visualization & Analytics
- **Tableau**: Interactive dashboards and BI
- **SQL**: Data validation and quality checks

### Data Sources
- **USDA/Kaggle**: US agricultural sales (2019-2023)
- **FAOSTAT**: Global FAO production statistics

---

## Project Structure

```
agrisight/
├── README.md                          # This file
├── notebooks/
│   └── AgriSight.ipynb               # Complete data pipeline notebook
├── sql/
│   ├── area_codes.sql                # Area code reference data
│   ├── production.sql                # FAOSTAT production data
│   ├── elements.sql                  # Element definitions
│   └── flags.sql                     # Data quality flags
├── config/
│   └── [Azure Data Factory configs]  # Pipeline definitions
├── pipeline/
│   └── FAOSTAT.json                  # FAOSTAT Pipeline action documentation
    └── US_data.json                  # US_data Pipeline action documentation
├── databricks/
│   └── data_overboard.ipynb          # Complete data cleaning, analysis notebook
├── dashboards/
│   └── [Tableau dashboard files]     # BI visualization exports
├── data/
│   ├── bronze/                       # Raw ingested data
│   ├── silver/                       # Cleaned and enriched
│   └── gold/                         # Business-level aggregates
└── docs/
    ├── ARCHITECTURE.md               # Detailed system design
    ├── DATA_DICTIONARY.md            # Feature definitions
```

---

## Quick Start

### Prerequisites
- Python 3.8+
- Azure subscription with Databricks workspace
- Tableau (for visualization)
- Git

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/agrisight.git
cd agrisight
```

2. **Set up Python environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Configure Azure credentials**
```bash
# Set environment variables
export AZURE_STORAGE_CONNECTION_STRING="your_connection_string"
export AZURE_SQL_SERVER="your_server_name"
export AZURE_SQL_DATABASE="your_database_name"
```

4. **Run the data pipeline**
```bash
# Using Azure Data Factory (recommended for production)
# Or locally with Databricks Connect:
databricks configure --token
python notebooks/AgriSight.ipynb
```

5. **Connect Tableau to gold layer**
- Configure Tableau data source pointing to gold layer CSV output
- Load pre-built dashboard templates from `/dashboards`

---

## Data Dictionary

### Core Features (46 total)

#### Price Metrics
| Feature | Description | Range |
|---------|-------------|-------|
| `avg_sale_price` | Mean price per product per year per state | $2.50–$15.99/lb |
| `price_yoy_change` | Year-over-year price change | -11.25% to +12.94% |
| `rolling_avg_price_2y` | 2-year rolling average for trend smoothing | Varies |
| `price_volatility_index` | Standard deviation of price over time | Varies |

#### Supply & Volume Metrics
| Feature | Description | Range |
|---------|-------------|-------|
| `total_quantity_sold_lb` | Aggregated sales volume in pounds | 0–3.5M lbs |
| `production_yoy_growth` | Annual production growth rate | -27.41% to +52.80% |
| `demand_supply_index` | Sales to production ratio (market balance) | 0.0–1.0 |

#### Production Efficiency
| Feature | Description |
|---------|-------------|
| `production_per_area` | Yield efficiency (tons per hectare) |
| `land_utilization` | % of arable land used for specific crops |

#### Financial Metrics
| Feature | Description | Typical Range |
|---------|-------------|---|
| `profit_margin_pct` | Gross margin percentage | 44–45% |
| `revenue_total` | Total revenue by product/region | Varies |

---

## Data Quality Validation

### Completeness
- **Matched records**: 99.8% completeness (1,125 records)
- **Unmatched records**: 98.5% completeness (525 records)
- **Overall usability**: 100%

### Outlier Analysis
| Feature | Outliers | % of Total | Reason |
|---------|----------|-----------|--------|
| `avg_sale_price` | 0 | 0.0% | No outliers detected |
| `price_yoy_change` | 18 | 1.1% | Normal market volatility |
| `production_yoy_growth` | 60 | 3.6% | Crop failures & bumper harvests (retained as valid) |

---

## Analysis Results

### Correlation with Price (Top 10 Features)
1. **Rolling 2-year avg price**: r = 0.999 ⭐ (dominant predictor)
2. **Production per area**: r = 0.24
3. **Demand-supply index**: r = -0.25
4. **Price YoY change**: r = 0.18
5. **Total quantity sold**: r = 0.12

### Key Insights
- **Price Inertia**: Historical prices explain 99.9% of current price variation
- **External Market Forces**: Internal metrics (margin, volume growth) show weak correlation with price
- **Supply-Demand Dynamics**: Oversupply significantly depresses commodity prices
- **Geographic Variation**: State-level differences reflect regional growing conditions and market access

---

## Use Cases

### For Farmers & Producers
- ✅ Benchmark state-level performance
- ✅ Forecast price trends for crop planning
- ✅ Identify high-performing regions and products
- ✅ Monitor year-over-year growth metrics

### For Traders & Commodity Merchants
- ✅ Identify price patterns and anomalies
- ✅ Analyze supply-demand imbalances
- ✅ Regional market comparison and arbitrage opportunities
- ✅ Risk hedging based on price inertia patterns

### For Policymakers & Supply Chain
- ✅ Monitor food supply chain health
- ✅ Identify regional concentration risks (California = 24% of US volume)
- ✅ Inform agricultural policy and trade decisions
- ✅ Forecast food price inflation drivers

---

## Future Enhancements

### Near-Term (Q1-Q2 2026)
- [ ] Real-time data integration via Apache Kafka
- [ ] Machine learning forecasting (ARIMA, Prophet, LSTM)
- [ ] Automated anomaly detection
- [ ] Mobile-friendly dashboard

### Medium-Term (Q3-Q4 2026)
- [ ] Global data expansion (EU, India, Brazil)
- [ ] Weather data integration
- [ ] Sentiment analysis on commodity news
- [ ] Cost optimization and auto-scaling

### Long-Term (2027+)
- [ ] Advanced ML (XGBoost, Deep Learning)
- [ ] Supply chain optimization models
- [ ] API for third-party integrations
- [ ] Open-source data lake

---

## Documentation

- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** - Detailed system design and medallion pattern implementation
- **[DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md)** - Complete feature definitions and data lineage

---

## Paper & Publication

This project is documented in a peer-reviewed LLNCS paper:

**AgriSight: Cloud-Native Agricultural Commodity Price Analytics & Forecasting**
- Authors: Shuvro Sankar Sen, Irtiza Ahsan Abir, Ahnaf Abdullah Zayad, Sandip Misra
- Institution: American International University-Bangladesh (AIUB)
- Dataset: 64.4M lbs US agricultural sales (2019-2023) + FAOSTAT global production
- See: `Springer_Lecture_Notes_in_Computer_Science-2.pdf`

---

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 Python style guide
- Add docstrings to all functions
- Include unit tests for new features
- Update documentation for changes

---

## Performance Metrics

| Metric | Value | Notes |
|--------|-------|-------|
| **Data Pipeline Latency** | < 15 min | End-to-end bronze to gold |
| **Data Enrichment Rate** | 68.2% | 1,125 of 1,650 records matched |
| **Query Performance** | < 2 sec | Tableau dashboard loads |
| **Data Quality Score** | 99.8% | Matched records completeness |
| **System Availability** | 99.5% | Azure SLA + monitoring |

---

## Security & Compliance

- ✅ Azure Role-Based Access Control (RBAC)
- ✅ Data encryption at rest (AES-256)
- ✅ Data encryption in transit (TLS 1.2+)
- ✅ Audit logging for all data access
- ✅ GDPR-compliant data handling
- ✅ No personally identifiable information (PII)

---



## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- American International University-Bangladesh (AIUB) for computational resources
- USDA & Kaggle for agricultural sales data
- Food and Agriculture Organization (FAO) for FAOSTAT production metrics
- Microsoft Azure for cloud infrastructure
- Databricks and Tableau communities for excellent documentation

---

## References

- Box, G.E.P., & Jenkins, G.M. (1976). *Time Series Analysis: Forecasting and Control*
- Chen, T., & Guestrin, C. (2016). XGBoost: A Scalable Tree Boosting System
- Databricks (2021). The Medallion Architecture
- Hochreiter, S., & Schmidhuber, J. (1997). Long Short-Term Memory
- Zhang, G.P. (2003). Time series forecasting using a hybrid ARIMA and neural network model

---

**Last Updated**: January 15, 2026  
**Maintainer**: [Shuvro Sankar Sen]  

---

<div align="center">




</div>
