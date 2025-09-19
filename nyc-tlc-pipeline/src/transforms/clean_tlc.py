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
