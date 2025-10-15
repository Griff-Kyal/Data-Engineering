import pandas as pd
from dotenv import load_dotenv
from io import StringIO
import os

load_dotenv("local.env")
DATA_DIR = os.getenv("DATA_DIR")

ds = (f"{DATA_DIR}/raw/charts.csv")
out_dir = "spotify-charts-pipeline/reports/"
out_ds = os.path.join(out_dir, "initial_data_profile.txt")

# ensure reports directory exists
os.makedirs(out_dir, exist_ok=True)

# read only header and a few rows
df_head = pd.read_csv(ds, nrows=10)

# capture output
columns_list = df_head.columns.tolist()
sample_rows = df_head.head().to_string()

# capture df.info() output
buffer = StringIO()
df_head.info(buf=buffer)
column_info = buffer.getvalue()

# write everything to file
with open(out_ds, "w", encoding="utf-8") as f:
    f.write("=== Spotify Charts: Column List ===\n")
    f.write(", ".join(columns_list) + "\n\n")
    f.write("=== DataFrame Info ===\n")
    f.write(column_info + "\n")
    f.write("=== Sample Rows ===\n")
    f.write(sample_rows + "\n")

print(f"Data profile written to: {out_ds}")
