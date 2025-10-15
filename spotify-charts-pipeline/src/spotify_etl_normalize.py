"""
Normalize Spotify charts data into dimension and fact tables
"""

import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv("spotify-charts-pipeline/local.env")

DATA_DIR = os.getenv("DATA_DIR")
RAW_FILE = Path(f"{DATA_DIR}/raw/charts.csv")
OUTPUT_DIR = Path(f"{DATA_DIR}/splits")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 100000


def load_raw_data(filepath, chunk_sz=None):
    """Load and clean the raw CSV"""
    print("Loading raw CSV...")
    
    if chunk_sz:
        chunks = []
        for chunk in pd.read_csv(filepath, chunksize=chunk_sz):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
    else:
        df = pd.read_csv(filepath)
    
    # rename some columns to be clearer
    df.rename(columns={
        'title': 'track_name',
        'rank': 'chart_position', 
        'date': 'chart_date',
        'artist': 'artist_name',
        'region': 'country_name'
    }, inplace=True)
    
    # only want top200, not viral50
    df = df[df['chart'].str.contains('top200', na=False)].copy()
    
    # basic cleanup
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df["artist_name"] = df["artist_name"].str.strip()
    df["track_name"] = df["track_name"].str.strip()
    df["country_name"] = df["country_name"].str.strip()
    df["chart_date"] = pd.to_datetime(df["chart_date"], errors="coerce").dt.date
    df['streams'] = df['streams'].fillna(0).astype(int)
    
    print(f"Loaded {len(df):,} rows")
    return df


def extract_artists(df):
    print("Building artists table...")
    artists = df[["artist_name"]].drop_duplicates().reset_index(drop=True)
    artists["artist_id"] = artists.index + 1
    return artists[["artist_id", "artist_name"]]


def extract_regions(df):
    regions = df[["country_name"]].drop_duplicates().sort_values("country_name").reset_index(drop=True)
    regions["region_id"] = regions.index + 1
    return regions[["region_id", "country_name"]]


def extract_tracks(df, artists):
    # need artist_id first
    df = df.merge(artists, on="artist_name", how="left")
    
    tracks = df[["track_name", "artist_id", "url"]].drop_duplicates(
        subset=['track_name', 'artist_id']
    ).reset_index(drop=True)
    
    tracks["track_id"] = tracks.index + 1
    return tracks[["track_id", "track_name", "artist_id", "url"]]


def build_fact_table(df, artists, tracks, regions):
    print("Building fact table...")
    
    # merge everything
    df = df.merge(artists, on="artist_name", how="left")
    df = df.merge(regions, on="country_name", how="left")
    df = df.merge(
        tracks[["track_id", "track_name", "artist_id"]],
        on=["track_name", "artist_id"],
        how="left"
    )
    
    # grab what we need
    fact = df[[
        "artist_id", "track_id", "artist_name", "track_name",
        "region_id", "country_name", "chart_date", 
        "chart_position", "streams"
    ]].copy()
    
    fact["entry_id"] = range(1, len(fact) + 1)
    
    print(f"Created {len(fact):,} chart entries")
    
    # reorder columns
    return fact[[
        "entry_id", "artist_id", "track_id", "artist_name",
        "track_name", "region_id", "country_name", "chart_date",
        "chart_position", "streams"
    ]]


def save_tables(artists, tracks, regions, entries):
    print("Saving to disk...")
    
    # dimension tables as CSV
    artists.to_csv(OUTPUT_DIR / "artists.csv", index=False)
    tracks.to_csv(OUTPUT_DIR / "tracks.csv", index=False)
    regions.to_csv(OUTPUT_DIR / "region.csv", index=False)
    
    # full version with names (for reference/debugging)
    entries.to_parquet(OUTPUT_DIR / "chart_entries_full.parquet", index=False)
    
    # normalized version (IDs only, for DB load)
    normalized = entries[[
        "entry_id", "artist_id", "track_id", "region_id",
        "chart_date", "chart_position", "streams"
    ]]
    normalized.to_parquet(OUTPUT_DIR / "chart_entries_normalized.parquet", index=False)
    
    print(f"Done! Files saved to {OUTPUT_DIR}")


def main():
    df = load_raw_data(RAW_FILE, CHUNK_SIZE)
    
    artists = extract_artists(df)
    regions = extract_regions(df)
    tracks = extract_tracks(df, artists)
    entries = build_fact_table(df, artists, tracks, regions)
    
    save_tables(artists, tracks, regions, entries)


if __name__ == "__main__":
    main()