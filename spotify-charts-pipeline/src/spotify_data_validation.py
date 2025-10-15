"""
Validate Spotify data between files and PostgreSQL
"""

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv("local.env")

DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS")
}

DATA_DIR = os.getenv("DATA_DIR") + "/splits"

# Check for clean file first, fall back to original
chart_file = f"{DATA_DIR}/chart_entries_normalized_clean.parquet"
if not os.path.exists(chart_file):
    chart_file = f"{DATA_DIR}/chart_entries_normalized.parquet"
    print(f"Warning: Using original file (no clean version found)")
    print(f"Run the loader first to create clean version\n")

FILES = {
    "artists": f"{DATA_DIR}/artists.csv",
    "tracks": f"{DATA_DIR}/tracks.csv",
    "region": f"{DATA_DIR}/region.csv",
    "chart_entries": chart_file
}


def count_rows_file(filepath):
    """Get row count from CSV or parquet"""
    if filepath.endswith('.parquet'):
        df = pd.read_parquet(filepath)
        return len(df)
    else:
        # faster than loading whole CSV
        return sum(1 for _ in open(filepath, encoding="utf-8")) - 1


def count_rows_db(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def check_row_counts(cur):
    print("\nRow counts:")
    all_match = True
    
    for table, filepath in FILES.items():
        file_cnt = count_rows_file(filepath)
        db_cnt = count_rows_db(cur, table)
        match = file_cnt == db_cnt
        all_match = all_match and match
        
        status = "✓" if match else f"✗ (file: {file_cnt:,}, db: {db_cnt:,})"
        print(f"  {table:15} {status}")
    
    return all_match


def check_foreign_keys(cur):
    print("\nForeign key integrity:")
    all_ok = True
    
    fk_checks = [
        ("artist_id", "artists"),
        ("track_id", "tracks"),
        ("region_id", "region")
    ]
    
    for col, ref_table in fk_checks:
        cur.execute(f"""
            SELECT COUNT(*) FROM chart_entries ce
            LEFT JOIN {ref_table} t ON ce.{col} = t.{col}
            WHERE t.{col} IS NULL;
        """)
        missing = cur.fetchone()[0]
        all_ok = all_ok and (missing == 0)
        
        status = "✓" if missing == 0 else f"✗ {missing:,} orphaned"
        print(f"  {col:15} {status}")
    
    return all_ok


def check_nulls(cur):
    print("\nNull checks:")
    cur.execute("""
        SELECT COUNT(*) FROM chart_entries
        WHERE chart_date IS NULL OR chart_position IS NULL OR streams IS NULL;
    """)
    nulls = cur.fetchone()[0]
    status = "✓" if nulls == 0 else f"✗ {nulls:,} null values"
    print(f"  critical fields {status}")
    return nulls == 0


def check_duplicates(cur):
    print("\nDuplicate checks:")
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT artist_id, track_id, region_id, chart_position, chart_date
            FROM chart_entries
            GROUP BY artist_id, track_id, region_id, chart_position, chart_date
            HAVING COUNT(*) > 1
        ) dup;
    """)
    dups = cur.fetchone()[0]
    
    if dups > 0:
        # show some examples
        cur.execute("""
            SELECT artist_id, track_id, region_id, chart_position, chart_date, COUNT(*) as cnt
            FROM chart_entries
            GROUP BY artist_id, track_id, region_id, chart_position, chart_date
            HAVING COUNT(*) > 1
            LIMIT 3;
        """)
        print(f"  chart entries   ✗ {dups:,} duplicates")
        print("  Examples:")
        for row in cur.fetchall():
            print(f"    artist={row[0]}, track={row[1]}, region={row[2]}, chart_position={row[3]} date={row[4]} (x{row[5]})")
    else:
        print(f"  chart entries   ✓")
    
    return dups == 0


def spot_check_data(cur):
    """Compare a random row between file and DB"""
    print("\nSpot check:")
    
    # grab random row from parquet and check columns
    df = pd.read_parquet(FILES["chart_entries"])
    print(f"  Parquet columns: {list(df.columns)}")
    
    sample = df.sample(1).iloc[0]
    print("  sample record:", ",        ".join(map(str, [x.item() if hasattr(x, 'item') else x for x in sample.to_list()])))
    
    # find same row in DB (use whatever column names exist in parquet)
    try:
        artist_id = int(sample['artist_id'])
        track_id = int(sample['track_id'])
        region_id = int(sample['region_id'])
        chart_position = int(sample['chart_position'])
        chart_date = str(sample['chart_date'])[:10]
        
        cur.execute("""
            SELECT artist_id, track_id, region_id, chart_position, streams, chart_date
            FROM chart_entries
            WHERE artist_id = %s AND track_id = %s 
            AND region_id = %s AND chart_position = %s AND chart_date = %s
            LIMIT 1;
        """, (artist_id, track_id, region_id, chart_position, chart_date))
        
        result = cur.fetchone()
        
        if result:
            print(f"  random record   ✓ found in DB")
            return True
        else:
            print(f"  random record   ✗ not found in DB")
            return False
            
    except KeyError as e:
        print(f"  random record   ✗ column missing: {e}")
        return False


def run_validation():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print("\n" + "=" * 50)
    print("Spotify Data Validation")
    print("=" * 50)
    
    checks = []
    checks.append(("Row counts", check_row_counts(cur)))
    checks.append(("Foreign keys", check_foreign_keys(cur)))
    checks.append(("Null values", check_nulls(cur)))
    checks.append(("Duplicates", check_duplicates(cur)))
    checks.append(("Spot check", spot_check_data(cur)))
    
    cur.close()
    conn.close()
    
    print("\n" + "=" * 50)
    print("Summary:")
    all_passed = all(result for _, result in checks)
    for name, passed in checks:
        print(f"  {name:15} {'PASS' if passed else 'FAIL'}")
    
    print("\n" + ("All validations passed!" if all_passed else "Some validations failed."))
    print("=" * 50)


if __name__ == "__main__":

    run_validation()
