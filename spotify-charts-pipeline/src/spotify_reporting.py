"""
Generate some reports from the Spotify charts database
"""

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv("local.env")

# config
DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS")
}

EXPORT_DIR = os.getenv("REPORT_DIR")
os.makedirs(EXPORT_DIR, exist_ok=True)

# report parameters - change these as needed
YEAR = 2021
CHART_DATE = "2021-12-07"
REGION_ID = 66 


def get_top_tracks(conn, year):
    """Top 10 tracks by total streams for a year"""
    query = f"""
        SELECT 
            t.track_name,
            a.artist_name,
            SUM(c.streams) AS total_streams
        FROM chart_entries c
        JOIN tracks t ON c.track_id = t.track_id
        JOIN artists a ON t.artist_id = a.artist_id
        WHERE EXTRACT(YEAR FROM c.chart_date) = {year}
        GROUP BY t.track_name, a.artist_name
        ORDER BY total_streams DESC
        LIMIT 10;
    """

    df = pd.read_sql_query(query, conn)
    
    df['total_streams'] = df['total_streams'].astype(int).apply(lambda x: f"{x:,}")
    
    return df


def get_top_artists(conn):
    """Artists with most unique songs"""
    query = """
        SELECT 
            a.artist_name,
            COUNT(DISTINCT t.track_id) AS unique_song_count
        FROM artists a
        JOIN tracks t ON a.artist_id = t.artist_id
        GROUP BY a.artist_name
        ORDER BY unique_song_count DESC
        LIMIT 50;
    """
    return pd.read_sql_query(query, conn)


def get_chart_snapshot(conn, CHART_DATE, region):
    """Get top 200 for a specific date and region"""
    query = f"""
        SELECT 
            c.entry_id,
            c.chart_date,
            c.chart_position,
            t.track_id,
            a.artist_id,
            t.track_name,            
            a.artist_name,
            r.region_id,
            r.country_name,
            c.streams
        FROM chart_entries c
        LEFT JOIN tracks t ON c.track_id = t.track_id
        LEFT JOIN artists a ON c.artist_id = a.artist_id
        LEFT JOIN region r ON c.region_id = r.region_id
        WHERE c.chart_date = '{CHART_DATE}'
          AND c.region_id = {region}
        ORDER BY c.chart_position ASC
        LIMIT 200;
    """
    df = pd.read_sql_query(query, conn)
    
    # Convert to int first to remove decimals, then format with commas
    df['streams'] = df['streams'].astype(int).apply(lambda x: f"{x:,}")
    
    return df


def main():
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # report 1 - top tracks for year
        print(f"Generating top tracks for {YEAR}...")
        df = get_top_tracks(conn, YEAR)
        output = os.path.join(EXPORT_DIR, f"top_10_tracks_{YEAR}.csv")
        df.to_csv(output, index=False)
        print(f"  Saved: {output}")
        
        # report 2 - artists by song count
        print("Generating top artists by unique songs...")
        df = get_top_artists(conn)
        output = os.path.join(EXPORT_DIR, "top_50_artists_by_songs.csv")
        df.to_csv(output, index=False)
        print(f"  Saved: {output}")
        
        # report 3 - chart snapshot
        print(f"Generating chart for {CHART_DATE} (region {REGION_ID})...")
        df = get_chart_snapshot(conn, CHART_DATE, REGION_ID)
        output = os.path.join(EXPORT_DIR, f"top_200_{CHART_DATE}_region{REGION_ID}.csv")
        df.to_csv(output, index=False)
        print(f"  Saved: {output}")
        
        print("\nAll reports generated")
        
    finally:
        conn.close()


if __name__ == "__main__":

    main()
