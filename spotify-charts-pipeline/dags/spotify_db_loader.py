"""
Load Spotify chart data into PostgreSQL with yearly partitioning.
Handles CSVs for dimension tables and parquet for the main fact table.
"""

import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from io import StringIO

load_dotenv("airflow.env")

# DB connection params
DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS")
}

DATA_DIR = os.getenv("DATA_DIR") + "/splits"

TABLE_FILES = {
    "artists": f"{DATA_DIR}/artists.csv",
    "tracks": f"{DATA_DIR}/tracks.csv",
    "region": f"{DATA_DIR}/region.csv",
    "chart_entries": f"{DATA_DIR}/chart_entries_normalized.parquet"
}

CHUNK_SIZE = 135000

def dedupe_chart_entries(parquet_path):
    """Remove duplicates from chart entries, keeping highest streams"""
    print(f"\nChecking for duplicates in {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    original_count = len(df)
    
    key_cols = ['artist_id', 'track_id', 'region_id', 'chart_position', 'chart_date']
    dupes = df.duplicated(subset=key_cols, keep=False)
    dupe_count = dupes.sum()
    
    if dupe_count > 0:
        print(f"  Found {dupe_count:,} duplicate rows")
        
        # show a couple examples
        print("  Sample duplicates:")
        sample = df[dupes].head(4)[key_cols + ['streams']]
        for _, row in sample.iterrows():
            print(f"    artist={row['artist_id']}, track={row['track_id']}, "
                  f"region={row['region_id']}, chart_position={row['chart_position']}, date={row['chart_date']}, streams={row['streams']}")
        
        # keep highest streams
        print("  Deduplicating (keeping highest streams)...")
        df = df.sort_values('streams', ascending=False).drop_duplicates(
            subset=key_cols,
            keep='first'
        )
        
        removed = original_count - len(df)
        print(f"  Removed {removed:,} duplicates")
        
        # save clean version
        clean_path = parquet_path.replace('.parquet', '_clean.parquet')
        print(f"  Saving clean data to {clean_path}")
        df.to_parquet(clean_path)
        
        return df, clean_path
    else:
        print("  No duplicates found")
        return df, parquet_path


def setup_schema(cur):
    """Drop and recreate all tables"""
    # clean slate
    cur.execute("DROP TABLE IF EXISTS chart_entries CASCADE;")
    cur.execute("DROP TABLE IF EXISTS tracks CASCADE;")
    cur.execute("DROP TABLE IF EXISTS artists CASCADE;")
    cur.execute("DROP TABLE IF EXISTS region CASCADE;")
    
    # dimension tables first
    cur.execute("""
        CREATE TABLE artists (
            artist_id INT PRIMARY KEY,
            artist_name TEXT
        );
    """)
    
    cur.execute("""
        CREATE TABLE tracks (
            track_id INT PRIMARY KEY,
            track_name TEXT,
            artist_id INT REFERENCES artists(artist_id),
            url TEXT
        );
    """)
    
    cur.execute("""
        CREATE TABLE region (
            region_id INT PRIMARY KEY,
            country_name TEXT
        );
    """)
    
    # main table with partitioning
    cur.execute("""
        CREATE TABLE chart_entries (
            entry_id INT,
            artist_id INT REFERENCES artists(artist_id),
            track_id INT REFERENCES tracks(track_id),
            region_id INT REFERENCES region(region_id),
            chart_date DATE NOT NULL,
            chart_position INT,
            streams BIGINT,
            PRIMARY KEY (entry_id, chart_date)
        ) PARTITION BY RANGE (chart_date);
    """)


def setup_partitions(cur, parquet_file):
    """Figure out year range and create partitions"""
    df = pd.read_parquet(parquet_file)
    df['chart_date'] = pd.to_datetime(df['chart_date'])
    
    start_year = df['chart_date'].min().year
    end_year = df['chart_date'].max().year
    
    print(f"Creating partitions for {start_year}-{end_year}...")
    
    for yr in range(start_year, end_year + 1):
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS chart_entries_{yr}
            PARTITION OF chart_entries
            FOR VALUES FROM ('{yr}-01-01') TO ('{yr+1}-01-01');
        """)
        print(f"  - chart_entries_{yr}")


def load_csv(cur, table, filepath):
    """Standard CSV load with COPY"""
    if not os.path.exists(filepath):
        print(f"Warning: {filepath} not found")
        return
    
    print(f"Loading {table}...", end=" ")
    with open(filepath, "r", encoding="utf-8") as f:
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV HEADER", f)
    print("done")


def load_parquet_chunked(cur, conn, table, filepath, chunk_sz=CHUNK_SIZE):
    """Load big parquet files in chunks to avoid memory issues"""
    if not os.path.exists(filepath):
        print(f"Warning: {filepath} not found")
        return
    
    # dedupe first and get clean file path
    result = dedupe_chart_entries(filepath)
    if isinstance(result, tuple):
        df, clean_path = result
    else:
        df = result
        clean_path = filepath
    
    total = len(df)
    chunks = (total + chunk_sz - 1) // chunk_sz
    
    print(f"\nLoading {table} ({total:,} rows in {chunks} chunks)...")
    
    for i in range(0, total, chunk_sz):
        chunk = df.iloc[i:i+chunk_sz]
        buf = StringIO()
        chunk.to_csv(buf, index=False, header=False)
        buf.seek(0)
        
        cur.copy_from(buf, table, sep=',', null='')
        conn.commit()
        
        chunk_num = i // chunk_sz + 1
        print(f"  [{chunk_num}/{chunks}] {len(chunk):,} rows")


def show_stats(cur):
    """Quick count check on all tables"""
    tables = ["artists", "tracks", "region", "chart_entries"]
    print("\nRow counts:")
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        cnt = cur.fetchone()[0]
        print(f"  {t}: {cnt:,}")
    
    # partition sizes
    print("\nPartition sizes:")
    cur.execute("""
        SELECT tablename, 
               pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
        FROM pg_tables
        WHERE tablename LIKE 'chart_entries_%'
        ORDER BY tablename;
    """)
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        print("Setting up schema...")
        setup_schema(cur)
        conn.commit()
        
        setup_partitions(cur, TABLE_FILES["chart_entries"])
        conn.commit()
        
        print("\nLoading dimension tables...")
        load_csv(cur, "artists", TABLE_FILES["artists"])
        conn.commit()
        
        load_csv(cur, "tracks", TABLE_FILES["tracks"])
        conn.commit()
        
        load_csv(cur, "region", TABLE_FILES["region"])
        conn.commit()
        
        print("\nLoading fact table...")
        load_parquet_chunked(cur, conn, "chart_entries", TABLE_FILES["chart_entries"])
        
        show_stats(cur)
        print("\nAll done!")
        
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()