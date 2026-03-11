\# Environmental Monitoring Data Quality Pipeline



An automated data validation framework for environmental field records. Built to validate the kind of messy, real-world data that restoration programs collect in the field tree inventories, biodiversity surveys, soil measurements, and GPS waypoints.



\---



!\[Dashboard](assets/screenshot\_dashboard.png)



\---



\## The Problem



Restoration programs like WRI TerraFund collect data from hundreds of local champions in the field. That data arrives with missing values, invalid GPS coordinates, out-of-range measurements, and duplicate record IDs. Without a systematic validation layer, bad data silently corrupts downstream analysis and erodes funder confidence.



This pipeline catches those problems before they reach the database.



\---



\## What It Does



Upload any CSV of environmental field records and get back an overall data quality score, pass/fail results for 25+ validation rules, a table of every flagged record with the specific rule it failed, remediation recommendations for each error type, and clean/flagged records as separate downloadable CSVs.



\---



\## Validation Rules



!\[Rules](assets/screenshot\_rules.png)



25+ rules across 8 categories:



| Category | Rules |

|----------|-------|

| Required fields | record\_id, region, survey\_type, lat, lon, date, surveyor not null |

| GPS bounds | lat -35 to 37, lon -18 to 52 (Africa region) |

| Measurement ranges | canopy 0-100%, height 0-100m, soil pH 0-14, biomass 0-100000 kg |

| Duplicates | record\_id must be unique |

| Valid categories | survey\_type and region from approved value sets |

| Date format | survey\_date must match YYYY-MM-DD |

| Column types | lat/lon must be float |

| Row count | dataset must have at least 1 record |



\---



\## Dashboard



!\[Gauge](assets/screenshot\_gauge.png)



!\[Charts](assets/screenshot\_charts.png)



The left chart breaks down flagged records by error type. The right chart shows clean record distribution across all 10 restoration regions.



\---



\## Database Layer



Clean records load into PostgreSQL with ON CONFLICT DO NOTHING for idempotent runs. After loading, a window function query ranks regions by average canopy cover using RANK() OVER (ORDER BY AVG(canopy\_cover\_pct) DESC).



\---



\## Stack



| Layer | Technology |

|-------|-----------|

| Validation | Great Expectations 0.18+ |

| Language | Python 3.11 |

| Data | Pandas, NumPy |

| Database | PostgreSQL 16 |

| ORM | SQLAlchemy 2.0, psycopg2 |

| Dashboard | Streamlit, Plotly |



\---



\## Running Locally



git clone https://github.com/HEX027/env-monitoring-data-quality.git

cd env-monitoring-data-quality

pip install -r requirements.txt

python src/create\_demo\_data.py

python src/validate.py

python src/load\_db.py

streamlit run src/dashboard.py



\---



\## About



https://github.com/HEX027

