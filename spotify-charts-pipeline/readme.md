# 🎵 Spotify Charts Data Engineering Project

## Overview

This project is a complete end-to-end data engineering pipeline built around the [Spotify Charts dataset](https://www.kaggle.com/datasets/dhruvildave/spotify-charts).  
It takes the raw chart data, cleans and normalizes it, loads it into a PostgreSQL database and runs validation checks to make sure everything lines up correctly.

The goal is to show how a modern data pipeline can be built with open-source tools, from extraction and transformation to orchestration and validation, in a way that’s simple to extend or scale.

---

## Project Goals

- Extract raw Spotify chart data from CSV files  
- Clean and transform the data into a normalized relational format  
- Create lookup and fact tables (artists, tracks, regions, chart_entries)  
- Load everything efficiently into PostgreSQL  
- Run validation checks for data integrity and foreign key consistency 
- Run reporting within python using hard coded variables that can be changed for flexible reporting  
- Automate the full process using Airflow

---

## Data Pipeline Architecture

<img width="1006" height="648" alt="Data Pipeline drawio" src="https://github.com/user-attachments/assets/4567ea52-3ca2-4d63-9593-20ab0db62adf" />

---

## Data Modelling

<img width="1080" height="563" alt="Data Modelling" src="https://github.com/user-attachments/assets/177ffcc9-4bd9-42df-abb6-e513c13d7e2b" />

---

## Directory Structure

```
spotify-charts-pipeline/
│
├── data/
│   ├── raw/
│   │   └── charts.csv
│   └── splits/
│       ├── artists.csv
│       ├── tracks.csv
│       ├── region.csv
│       ├── chart_entries_normalized.parquet
│       └── chart_entries_full.parquet
│
├── dags/
│   ├── spotify_airflow_dag.py
│   ├── spotify_etl_normalize.py
│   ├── spotify_db_loader.py
│   ├── spotify_data_validation.py
│   └── spotify_reporting.py
│
├── scripts/
│   ├── spotify_etl_normalize.py
│   ├── spotify_db_loader.py
│   ├── spotify_data_validation.py
│   ├── data_profiler.py
│   └── spotify_reporting.py
│
├── logs/
│
├── plugins/
│
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── airflow.env
├── local.env
└── README.md
```

---

## Tech Stack

- **Python 3.10+** – ETL, transformation, validation  
- **PostgreSQL** – database for normalized tables  
- **Apache Airflow** – scheduling and workflow management  
- **Docker / Docker Compose** – containerized local setup  
- **Pandas, psycopg2, SQLAlchemy** – data manipulation and SQL connectivity  

---

## Getting Started

### 1. Clone the repo

You can clone just this repo using a sparse checkout with the commands below:

```bash
# shallow + blob filtering to minimize download (optional but recommended)
git clone --depth 1 --filter=blob:none --no-checkout https://github.com/Griff-Kyal/Data-Engineering.git
cd Data-Engineering

# enable sparse checkout in cone mode and set the folder you want
git sparse-checkout init --cone
git sparse-checkout set spotify-charts-pipeline

# finally checkout the branch (main)
git checkout main

cd spotify-charts-pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate       # Linux/Mac
venv\Scripts\activate          # Windows
```

### 3. Install dependencies

Here’s the `requirements.txt` contents:

```txt
pandas
pyarrow
psycopg2-binary
sqlalchemy
apache-airflow
python-dotenv
```

Then install everything:

```bash
pip install -r requirements.txt
```

### 4. Download the kaggle dataset

Download the [Spotify Charts dataset](https://www.kaggle.com/datasets/dhruvildave/spotify-charts) from the link, then place the csv file into to the `data/raw/` directory of the project.

---

## Setting Up the Environment

### PostgreSQL

You can either use your local PostgreSQL installation or spin up a container with Docker:

```yaml
# docker-compose.yml
version: '3.9'
services:
  postgres:
    image: postgres:15
    container_name: spotify_postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
      POSTGRES_DB: spotify
    ports:
      - "5432:5432"
    volumes:
      - ./postgres_data:/var/lib/postgresql/data
```

Start it with:

```bash
docker-compose up -d
```

### Environment Variables

Create a  `local.env` file, fill in and update your credentials:

```env
PG_USER=postgres
PG_PASS=secret
PG_HOST=localhost
PG_PORT=5432
PG_DB=spotify-db
DATA_DIR=/data
REPORT_DIR=/reports
```

---

## Airflow Setup (optional)

If you’d like to automate the pipeline with Airflow, There is a  (`docker-compose.yml`) which can be used which is pre-configured. To run this project, cd to the project directory in a cmd terminal and run the following commands:

Initialize Airflow (first time only)
```bash
docker-compose up airflow-init
```

Start Airflow
```bash
docker-compose up -d
```

Then open the Airflow UI at [http://localhost:8080](http://localhost:8080), allowing up to 30 seconds to fully load

You can stop Airflow anytime with the following command:
```bash
docker-compose down
```

### Environment Variables

Create a  `airflow.env` file, fill in and update your credentials:

```env
PG_USER=postgres
PG_PASS=secret
PG_HOST=host.docker.internal
PG_PORT=5432
PG_DB=spotify-db
DATA_DIR=/opt/airflow/data
REPORT_DIR=/opt/airflow/reports
```

---

## Data Profiler

You can get a profile of the initial data by manually running the data profiler python script

```bash
python src/data_profiler.py
```

Which will output a `initial_data_profile` text file in the reports directory, to give an idea on the base line column names, data types and sample data

---

## Running the Pipeline

### Option 1: Manual execution

```bash
python src/spotify_etl_normalize.py
python src/spotify_db_loader.py
python src/spotify_data_validation.py
python src/spotify_reporting.py
```

### Option 2: Airflow

Trigger the DAG `spotify_charts_etl` in the Airflow UI to run the full Extract → Load → Validate → Report sequence automatically.

---

## Data Transformation

The initial CSV file comes in at **3.24GB**, which contains unnecessary data. Using the `spotify_etl_normalize.py` are are able to make the following transformations to our initial Data Frame:

- Remove all records for the `Viral50` charts as only the `Top200` was required, then dropping the `chart` column completely.
- Creating an `artist` data frame, assigning each artists an artist_id, dropping duplicate appearances and replacing entries with the designated number.
- Creating an `tracks` data frame, assigning each track a track_id, dropping duplicate appearances and replacing entries with the designated number. Also linked the track_id to the artist_id to build a unique track and complying with space saving practices.
- Creating an `region` data frame, assigning each country a region_id, dropping duplicate appearances and replacing entries with the designated number.
- Creating an `chart_entries` data frame, where the normalized dataframe only contains the `artist_id, track_id & region_id`, while also partitioning the chart entries by year to reduce query times, combining tables with LEFT JOIN to grab all visual relevant data required later on.
- Column renaming and stripping for consistency with import into Postgre.  

The separated DataFrames are then split into different csv/parquet files for faster COPY into the database, with the outputted file sizes as the following:

-`artists.csv` **1.07MB**  
-`tracks.csv` **8.32MB**  
-`region.csv` **1KB**  
-`chart_entries_normalized.parquet` **226MB**  

This is a combined total of 235MB of data, saving around **3GB** of space and removing over 6 million un-needed rows of data and stripping unnecessary columns. 

---

## Reporting

There are variables that can be configured in the `spotify_reporting.py` file to alter the reports being generated. the example and default variables are below:

```python
YEAR = 2021
CHART_DATE = "2021-12-07"
REGION_ID = 66
```

The region_id defaulted is for `United Kingdom`, but a key for the regions can be found by simply running the following command in Postgre:

```sql
SELECT * FROM region
```

The example reports can be views here:

[top 10 tracks 2021.](https://github.com/Griff-Kyal/Data-Engineering/blob/main/spotify-charts-pipeline/reports/top_10_tracks_2021.csv)  
[top 50 artists by songs](https://github.com/Griff-Kyal/Data-Engineering/blob/main/spotify-charts-pipeline/reports/top_50_artists_by_songs.csv)  
[top 200 2021-12-07 region66](https://github.com/Griff-Kyal/Data-Engineering/blob/main/spotify-charts-pipeline/reports/top_200_2021-12-07_region66.csv)  

---

## Database Model

**Schema Overview**

```
artists(artist_id) ───< tracks(artist_id)
      │                      │
      └──< chart_entries(artist_id, track_id, region_id) >── region(region_id)
```

**PostgreSQL Tables**

```sql
CREATE TABLE artists (
    artist_id SERIAL PRIMARY KEY,
    artist_name TEXT
);

CREATE TABLE tracks (
    track_id SERIAL PRIMARY KEY,
    track_name TEXT,
    artist_id INT REFERENCES artists(artist_id),
    url TEXT
);

CREATE TABLE region (
    region_id SERIAL PRIMARY KEY,
    country_name TEXT
);

CREATE TABLE chart_entries (
    entry_id SERIAL PRIMARY KEY,
    artist_id INT REFERENCES artists(artist_id),
    track_id INT REFERENCES tracks(track_id),
    region_id INT REFERENCES region(region_id),
    chart_date DATE,
    position INT,
    streams BIGINT,
    url TEXT
);
```

---

## Data Validation

The validation script (`spotify_data_validation.py`) checks:

- Row counts between CSVs and SQL tables  
- Foreign key integrity (no missing references)  
- Null and duplicate values  
- Random sample comparison between CSV and database  

Run it manually:

```bash
python src/spotify_data_validation.py
```

---

## Monitoring and Logging

Airflow provides task-level logs, and each step can be extended to log validation results into a separate database table for audits or notifications (Slack, email, etc.).

---

## Scalability Notes

- For large files, the `spotify_db_loader.py` uses `read_csv(chunksize=135000)` and PostgreSQL’s `COPY` for faster loads and more stable memory allocation  
- Convert intermediate files to **Parquet** for better performance and compression  
- The same structure can be adapted to cloud databases (BigQuery, Snowflake)  

---

## Testing

| Stage | Check |
|--------|-------|
| **Extract** | Verify raw file is available |
| **Transform** | Validate schema and column names |
| **Load** | Compare row counts |
| **Validate** | Ensure no missing or duplicate data |

---

## Next Steps

- Deploy the pipeline to cloud infrastructure (AWS, GCP, etc.)  
- Build a lightweight dashboard (Metabase, Power BI) to visualize the data  
- Add monitoring and alerts for failed DAG runs  

---

## Author

**Kyal Griffiths**  
Aspiring Data Engineer | SQL & Python Developer   

🔗 [LinkedIn](https://www.linkedin.com/in/kyal-griffiths)



