import pandas as pd
import psycopg2
import os
from glob import glob

# === Config ===
DB_CONFIG = {
    "dbname": "footy_data",
    "user": "postgres",
    "password": "ArnaudZander10!",
    "host": "localhost",
    "port": "5432"
}

DATASETS_DIR = "DATASETS"

# === Helpers ===
def extract_season_from_filename(filename):
    basename = os.path.basename(filename)
    try:
        year_part = basename.split("_")[1].split(".")[0]
        year = int(year_part)
        return f"{year}/{str(year + 1)[-2:]}"
    except (IndexError, ValueError):
        return "Unknown"

def parse_date(date_str):
    """Handles common date formats"""
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except:
            continue
    return pd.to_datetime(date_str, errors='coerce')

# === Main Loader ===
def insert_match_data(df, season):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    successful = 0

    for i, row in df.iterrows():
        try:
            home_team = row["HomeTeam"]
            away_team = row["AwayTeam"]
            date = parse_date(row["Date"])

            if pd.isna(date):
                print(f"⚠️ Skipping invalid date: {row['Date']}")
                continue

            # Insert teams
            cur.execute("INSERT INTO teams (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (home_team,))
            cur.execute("INSERT INTO teams (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (away_team,))

            # Get team IDs
            cur.execute("SELECT team_id FROM teams WHERE name = %s", (home_team,))
            home_team_id = cur.fetchone()[0]
            cur.execute("SELECT team_id FROM teams WHERE name = %s", (away_team,))
            away_team_id = cur.fetchone()[0]

            # Insert match
            cur.execute("""
                INSERT INTO matches (date, season, home_team_id, away_team_id, venue, home_goals, away_goals, result)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING match_id
            """, (
                date, season, home_team_id, away_team_id, None,
                row["FTHG"], row["FTAG"], row["FTR"]
            ))
            match_id = cur.fetchone()[0]

            # Insert team stats
            cur.execute("""
                INSERT INTO team_stats (match_id, team_id, is_home_team, shots, shots_on_target, corners, fouls, yellow_cards, red_cards)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                match_id, home_team_id, True,
                row.get("HS", 0), row.get("HST", 0), row.get("HC", 0),
                row.get("HF", 0), row.get("HY", 0), row.get("HR", 0)
            ))

            cur.execute("""
                INSERT INTO team_stats (match_id, team_id, is_home_team, shots, shots_on_target, corners, fouls, yellow_cards, red_cards)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                match_id, away_team_id, False,
                row.get("AS", 0), row.get("AST", 0), row.get("AC", 0),
                row.get("AF", 0), row.get("AY", 0), row.get("AR", 0)
            ))

            conn.commit()
            successful += 1

        except Exception as e:
            conn.rollback()
            print(f"❌ Row {i} failed: {e}")
            print(row.to_dict())

    cur.close()
    conn.close()
    print(f"✅ Inserted {successful} matches for season {season}")


# === Main Entry ===
def main():
    csv_files = glob(os.path.join(DATASETS_DIR, "*.csv"))
    if not csv_files:
        print("⚠️ No CSV files found in DATASETS/")
        return

    for file_path in csv_files:
        print(f"📂 Loading: {file_path}")
        df = pd.read_csv(file_path)

        # Sanity check
        required_cols = {"HomeTeam", "AwayTeam", "Date", "FTHG", "FTAG", "FTR"}
        if not required_cols.issubset(df.columns):
            print(f"⚠️ Skipping {file_path} (missing required columns)")
            continue

        season = extract_season_from_filename(file_path)
        insert_match_data(df, season)

if __name__ == "__main__":
    main()
