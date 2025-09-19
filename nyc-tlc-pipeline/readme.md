# NYC Taxi Trip Data Pipeline :taxi:

## Objective
The goal of this project is to design and implement an end to end **data engineering pipeline** that consists of the following stages:  

1. Creating the scripts which will extract the data, clean unnecessary entries ready for further cleaning, then automating the process.  
2. Transformed and shaped the data ready for modelling using Python and Jupyter notebooks 
3. Set up the Postgre database using Docker, ready for loading using the sqlalchemy module
4. Create and model the database tables using PgAdmin and running SQL queries for analysing the data

---

## Table of Contents
- [Dataset Used](#dataset-used)  
- [Technologies](#technologies)  
- [Data Pipeline Architecture](#data-pipeline-architecture)  
- [Data Modelling](#data-modelling)  
- [Step 1: Data Extraction and Process Automation](#step-1-data-extraction-and-process-automation)  
- [Step 2: Transformation](#step-2-transformation)  
- [Step 3: Data Loading / Storage](#step-3-data-loading--storage)  
- [Step 4: Database Table Modelling & Analytics](#step-4-database-table-modelling--analytics)  

---

## Dataset Used
- **NYC Taxi & Limousine Commission (TLC) Trip Record Data**  
  Source: [NYC TLC Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
  - Yellow and Green Taxi trip data available monthly (Parquet format)  
  - Includes pickup/dropoff times, locations, fares, passenger counts, and payment types  
- **Yellow Taxi Data Dictionary**  
  Source: [NYC TLC Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
  

---

## Technologies
- **Python** (data ingestion, cleaning, orchestration)  
- **Pandas / DuckDB** (data exploration and lightweight transformations)  
- **Postgres** (data storage & analytics queries)  
- **Prefect** (orchestration of pipeline steps)  
- **dbt** (optional: SQL-based modeling & testing)  
- **Docker / docker-compose** (for local setup of Postgres + orchestration tools)  
- **GitHub Actions** (for CI/CD and testing)  

---

## Data Pipeline Architecture
![Pipeline Architecture](docs/pipeline.png) 

---

## Data Modelling
![Data Model](docs/data_model.png)  

---

## Step 1: Data Extraction and Process Automation
The first step was to create a config file, so we could have a centralised location to make process automation easier. The config will be the centralised place for the parquet url to be placed for automated extraction, as well as database configuration so database settings are not hard coded within the scripts.

>config.py

```python
#URL of NYC yellow trip data to extract
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-07.parquet"

#Postgres database information
dbusername = "postgres"
dbpassword = "secret"
dbhost = "localhost"
dbport = 5432
dbname = "postgres-db"


#Do not change
filename = os.path.basename(url)

date_part = filename.split("_")[2].replace(".parquet", "")
date_part = date_part.replace("-","_")
```
This also handles the naming convention of the master database and the cleaned data, meaning the link for a different month/years data can be entered and no hard coding amendments need to be made. 

Next, we need to create the code which will handle the extraction of the parquet file from the URL entered in the config file.

>src/injestion/download_tlc.py

```python
import requests
import os
import sys

# Path setup for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config import url, filename
   
try:
    os.makedirs("nyc-tlc-pipeline/data/raw", exist_ok=True)
    print("Directory created/exists")
       
    print("Starting download...")
    response = requests.get(url)
    response.raise_for_status()  # Raises an exception for bad status codes
       
    print(f"Download complete. Content size: {len(response.content)} bytes")
       
    with open(f"nyc-tlc-pipeline/data/raw/{filename}", "wb") as f:
        f.write(response.content)
           
    # Verify file was written
    file_size = os.path.getsize(f"nyc-tlc-pipeline/data/raw/{filename}")
    print(f"File saved successfully! Size: {file_size} bytes")
       
except requests.exceptions.RequestException as e:
    print(f"Network error: {e}")
except OSError as e:
    print(f"File system error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

Running this downloads the data onto our disk.

>data/raw/yellow_tripdata_2025-07.parquet

Opening the raw data shows 3,898,963 lines of data.

Now that we have the data, we need to clean it by removing any fare amounts that are either at 0, or in negative as they will not be useful for reporting and can save disk space by clearing unwanted data. We will also remove any 0.0 mile trips as we will count these as invalid.

>src/transforms/clean_tlc.py

```python
import duckdb
import os
import sys

# Path setup for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config import filename, date_part

# Load CSV/Parquet
con = duckdb.connect()
df = con.execute(f"SELECT * FROM 'nyc-tlc-pipeline/data/raw/{filename}'").df()

# Clean step: remove negative fares
df = df[df["fare_amount"] >= 0]

# Clean step: remove 0 mile trips
df = df[df["trip_distance"] > 0.1]

# Create output directory if it doesn't exist
os.makedirs("nyc-tlc-pipeline/data/cleaned", exist_ok=True)

# Save cleaned data
df.to_parquet(f"nyc-tlc-pipeline/data/cleaned/cleaned_trips_{date_part}.parquet")
print("Cleaned data saved!")
```

This now saves the cleaned data as a new file

> data/cleaned/cleaned_trips_2025_07

We now have 3,522,283 lines of data, making a difference in 376,680 removed which is crucial for consistent data as well as disk space saving practices.

Now we have confirmed both scripts to be working, its time to create the script that will automate the process using Python and the Prefect module.

>src/flows/tlc_flow.py

```python
from prefect import flow, task
import subprocess

@task
def download():
    subprocess.run(["python", "nyc-tlc-pipeline/src/ingestion/download_tlc.py"])

@task
def clean():
    subprocess.run(["python", "nyc-tlc-pipeline/src/transforms/clean_tlc.py"])

@flow
def pipeline():
    download()
    clean()

if __name__ == "__main__":
    pipeline()
```

We can now run this single file to automate the process of extracting the parquet file, cleaning the unnecessary data and outputting the required parquet file, with only a URL change in the config file when required.

---

## Step 2: Transformation

We have partly transformed the data previously, using Python with DuckDB to Handle missing or invalid records (negative fares, zero distance). Now we will use Python with Jupyter Notebooks to look further into the data to make further Transformations.

These transformations can be seen in the [cleaning_with_postgre_upload.ipynb](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) file. some of the transformations we make are:

  - Fix datatypes (timestamps, floats, integers)    
  - Column renaming
  - Column amendments for Postgre compatibility
  - Transforming the data in the payment type from numeric to string datatypes, then replace the values in line with the data dictionary.
  - Removed invalid data, where the pickup date fell outside of the 07/2025 parameters

After the clear up of the data, we had 3,522,276 lines of data.  
 
---

## Step 3: Data Loading / Storage

The next step is to set up docker to host the Postgre database for import. to do this, we ran the following commands within cmd to create the containers and volumes required for the database.

```cmd
docker run --name nyc-taxi-postgres -e POSTGRES_PASSWORD=secret -d postgres

docker exec -u postgres nyc-taxi-postgres createdb postgres-db
```

Once i had confirmation that the database has been created and I could log in, i then used Python with sqlalchemy and psycopg2 to create a variable which would connect to the database using the settings in the config file, to establish a heartbeat to the the database, then load the parquet dataframe into the database as a master table. This is also done at the bottom of the cleaning_with_postgre_upload.ipynb file.

```python
#Authenticating connection to Postgre and importing data into master_table
from sqlalchemy import create_engine
import psycopg2

engine = create_engine(f'postgresql://{dbusername}:{dbpassword}@{dbhost}:{dbport}/{dbname}')

df.to_sql(f'master_table_{date_part}, engine')
```

This query took 8min 51sec to complete, but successfully imported all 3,522,276 lines of data into our Postgre database. 

picture here

---

## Step 4: Database Table Modelling & Analytics

Now we have created the data, we can now use the queries within the [table_creation.sql](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) file to brake down the data and create the tables we need for the daily report analytics.

Now we have created all the relevant tables, we can run the queries within the [analytics_queries.sql](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to gather a brake down of the daily revenue, airport trips and mileage, then create a daily_summary table using the following query:

```sql
CREATE TABLE daily_summary AS
WITH revenue_cte AS (
    SELECT 
        pickup_day, 
        SUM(total_amount::numeric) AS revenue
    FROM trip_amount
    GROUP BY pickup_day
),
airport_cte AS (
    SELECT 
        pickup_day, 
        COUNT(airport_fee) AS total_airport_trips
    FROM airport_trips
    GROUP BY pickup_day
),
mileage_cte AS (
    SELECT 
        pickup_day, 
        SUM(trip_distance_miles::numeric) AS daily_mileage
    FROM trip_mileage
    GROUP BY pickup_day
)
SELECT 
    r.pickup_day,
    r.revenue,
    a.total_airport_trips,
    m.daily_mileage
FROM revenue_cte r
FULL OUTER JOIN airport_cte a ON r.pickup_day = a.pickup_day
FULL OUTER JOIN mileage_cte m ON COALESCE(r.pickup_day, a.pickup_day) = m.pickup_day
ORDER BY r.pickup_day NULLS LAST;
```

You can see the analytics below:



---