# HDX Ukraine — Humanitarian Data Pipeline

End-to-end ETL pipeline pulling live humanitarian datasets from the
[Humanitarian Data Exchange (HDX)](https://data.humdata.org/) into
MS SQL Server, with a structured schema ready for ArcGIS Pro and Streamlit.

---

## Architecture

```
HDX API / Local Files
        │
        ▼
  Python Extractors          ← pandas, type coercion, validation
        │
        ▼
  MS SQL Server              ← dbo.food_prices / five_w / fts_funding / hunger_map
        │
        ├──→ ArcGIS Pro      ← geospatial maps via arcpy
        └──→ Streamlit       ← interactive dashboard
```

## Data Sources

| Source | Table | Description |
|--------|-------|-------------|
| WFP Food Prices | `dbo.food_prices` | Market prices by commodity, lat/lon, 2014–present |
| 5W People Reached | `dbo.five_w` | People assisted per cluster per Oblast |
| FTS Cluster Funding | `dbo.fts_funding` | Requirements vs funding by cluster/year |
| WFP Hunger Map | `dbo.hunger_map` | Food Consumption Score prevalence over time |

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your SQL Server connection details
```

### 3. Initialize database schema
Open SSMS, connect to your server, and run:
```sql
-- Create the database first
CREATE DATABASE hdx_ukraine;
GO
-- Then run the schema file
```
Or run `db/schema.sql` directly from SSMS.

### 4. Place your data files
```
data/
└── raw/
    ├── wfp_food_prices_ukr.csv
    ├── ukraine-5w-2025-january-december-2025-12-31.xlsx
    ├── fts_requirements_funding_cluster_ukr.csv
    └── wfp-hungermap-data-for-ukr-long.csv
```

### 5. Run the pipeline

```bash
# Test DB connection
python pipeline.py --validate

# Dry run (extract only, no DB write)
python pipeline.py --dry-run

# Run all sources
python pipeline.py

# Run a single source
python pipeline.py --source food
python pipeline.py --source five_w
python pipeline.py --source funding
python pipeline.py --source hunger
```

## Sample Output

```
22:14:01  INFO     [pipeline]  HDX Ukraine Humanitarian Data Pipeline
22:14:01  INFO     [db]        Database connection OK
22:14:01  INFO     [pipeline]  SOURCE: FOOD  →  [food_prices]
22:14:02  INFO     [extractor.food_prices]  Raw rows: 14,832
22:14:02  INFO     [extractor.food_prices]  Clean rows: 14,798  |  date range: 2014-03-15 → 2025-11-01
22:14:02  INFO     [db]        Writing 14,798 rows → [food_prices]
22:14:03  INFO     [db]        ✓  [food_prices] written successfully
```

## Pipeline Run Log

Every execution is logged to `dbo.pipeline_runs`:
```sql
SELECT * FROM dbo.pipeline_runs ORDER BY run_at DESC;
```

## Next Steps

- [ ] Scheduled runs via Windows Task Scheduler or cron
- [ ] ArcGIS Pro connection to `dbo.food_prices` (lat/lon ready)
- [ ] Streamlit dashboard over `dbo.five_w` and `dbo.fts_funding`
- [ ] HDX API direct pull (replace local files with live API calls)
