from datetime import datetime
import pandas as pd
import sqlite3
import requests
from meteostat import Point, Daily
from epiweeks import Week
import os

# Constants
START_DATE = datetime(2020, 3, 1)
END_DATE = datetime(2023, 3, 1)
COVID_BASE_URL = "https://api.covidactnow.org/v2"

COVID_API_KEY_ENV = "COVID_ACT_NOW_API_KEY"
FLU_API_KEY_ENV = "FLUVIEW_API_KEY"

# Location Constants
MICHIGAN_LOCATION = Point(42.3314, -83.0458)  # Detroit, Michigan
NATIONAL_LOCATIONS = {
    "New York": Point(40.7128, -74.0060),
    "Los Angeles": Point(34.0522, -118.2437),
    "Chicago": Point(41.8781, -87.6298),
    "Houston": Point(29.7604, -95.3698),
    "Miami": Point(25.7617, -80.1918)
}

def create_database():
    """Create the database and all required tables if they don't exist"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "final_project.db")

        # Create a new database connection
        conn = sqlite3.connect(db_path)

        # Enable foreign key support
        conn.execute("PRAGMA foreign_keys = ON")

        # Create run counts table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS run_counts (
            table_name TEXT PRIMARY KEY,
            run_count INTEGER DEFAULT 0
        )
        """)
        
        # Create weather tables
        conn.execute("""
        CREATE TABLE IF NOT EXISTS national_weather_data (
            time TEXT PRIMARY KEY,
            tavg REAL,
            tmin REAL,
            tmax REAL
        )
        """)
        
        conn.execute("""
        CREATE TABLE IF NOT EXISTS michigan_weather_data (
            week_id INTEGER PRIMARY KEY,
            tavg_f REAL,
            tmin_f REAL,
            tmax_f REAL
        )
        """)
        
        # Create COVID tables
        conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_michigan_covid_data (
            date TEXT PRIMARY KEY,
            cases INTEGER
        )
        """)
        
        conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_michigan_covid_data (
            week_id INTEGER PRIMARY KEY,
            weekly_cases INTEGER
        )
        """)
        
        conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_national_covid_data (
            date TEXT PRIMARY KEY,
            cases INTEGER
        )
        """)
        
        conn.execute("""
        CREATE TABLE IF NOT EXISTS weekly_national_covid_data (
            week_id INTEGER PRIMARY KEY,
            weekly_cases INTEGER
        )
        """)

        # Create flu data table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS flu_data_march_2020_to_2023 (
            region_key INTEGER,
            date TEXT,
            week_id INTEGER,
            num_ili INTEGER,
            PRIMARY KEY (region_key, date)
        )
        """)

        conn.commit()
        print("Database and tables created successfully")
        return True

    except Exception as e:
        print(f"Error creating database: {str(e)}")
        return False

    finally:
        if conn:
            conn.close()

def get_db_connection():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "final_project.db")
    return sqlite3.connect(db_path)

def get_api_key(env_var_name):
    api_key = os.getenv(env_var_name)
    if not api_key:
        raise EnvironmentError(
            f"Missing API key. Please set {env_var_name} in your environment."
        )
    return api_key

def initialize_run_counts():
    conn = get_db_connection()
    conn.execute("""
    CREATE TABLE IF NOT EXISTS run_counts (
        table_name TEXT PRIMARY KEY,
        run_count INTEGER DEFAULT 0
    )
    """)
    conn.commit()
    conn.close()

def get_run_count(table_name):
    conn = get_db_connection()
    result = conn.execute("SELECT run_count FROM run_counts WHERE table_name = ?", (table_name,)).fetchone()
    conn.close()
    return result[0] if result else 0

def increment_run_count(table_name):
    conn = get_db_connection()
    conn.execute("""
    INSERT INTO run_counts (table_name, run_count)
    VALUES (?, 1)
    ON CONFLICT(table_name) DO UPDATE SET run_count = run_count + 1
    """, (table_name,))
    conn.commit()
    conn.close()

def get_table_row_count(table_name):
    conn = get_db_connection()
    result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
    conn.close()
    return result[0] if result else 0

def get_week_id(date):
    parsed_date = datetime.strptime(date, '%Y-%m-%d')
    return int(parsed_date.strftime('%Y%U'))

def process_weather_data(location, start_date, end_date, table_name):
    """
    Process weather data with proper run count handling:
    - Runs 1-4: Collect exactly 25 new rows per run
    - Run 5: Collect all remaining data
    - Runs 6+: No new data collection
    """
    try:
        conn = get_db_connection()
        run_count = get_run_count("national_weather_data")

        # Check if we already have complete data (run 5+)
        if run_count >= 5:
            print("Weather data already complete. Skipping collection.")
            return

        # Get current data count
        current_count = conn.execute("SELECT COUNT(*) FROM national_weather_data").fetchone()[0] or 0

        # Fetch data from API
        data = Daily(location, start_date, end_date).fetch()
        data.index = data.index.to_series().apply(pd.to_datetime)
        data.reset_index(inplace=True)
        data = data.rename(columns={"index": "time"})

        # For runs 1-4, limit to next 25 rows after current count
        if run_count < 4:
            start_idx = current_count
            end_idx = start_idx + 25
            data = data.iloc[start_idx:end_idx]

        # Insert new daily records
        for _, row in data.iterrows():
            conn.execute("""
            INSERT OR IGNORE INTO national_weather_data (time, tavg, tmin, tmax)
            VALUES (?, ?, ?, ?)
            """, (
                row['time'].strftime('%Y-%m-%d'),
                row.get('tavg'),
                row.get('tmin'),
                row.get('tmax')
            ))
        
        # Update weekly aggregation
        weekly_data = conn.execute(f"""
        WITH weekly_temps AS (
            SELECT
                CAST(strftime('%Y', time) AS INTEGER) * 100 + CAST(strftime('%W', time) AS INTEGER) AS week_id,
                AVG((tavg * 9/5) + 32) AS tavg_f,
                AVG((tmin * 9/5) + 32) AS tmin_f,
                AVG((tmax * 9/5) + 32) AS tmax_f
            FROM national_weather_data
            WHERE time BETWEEN ? AND ?
            GROUP BY week_id
            ORDER BY week_id
        )
        SELECT * FROM weekly_temps
        """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))).fetchall()
        
        for week in weekly_data:
            conn.execute(f"""
            INSERT OR REPLACE INTO "{table_name}" (week_id, tavg_f, tmin_f, tmax_f)
            VALUES (?, ?, ?, ?)
            """, week)

        conn.commit()

        # Print status
        new_count = conn.execute("SELECT COUNT(*) FROM national_weather_data").fetchone()[0]
        weekly_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        increment_run_count("national_weather_data")
        current_run = run_count + 1
        print(f"Weather data processing complete (Run {current_run}):")
        print(f"- Previous daily records: {current_count}")
        print(f"- New daily records: {new_count}")
        print(f"- Records added this run: {new_count - current_count}")
        print(f"- Weekly aggregated weather in {table_name}: {weekly_count} rows")

    except Exception as e:
        print(f"Error processing weather data: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def store_covid_data(data, table_name):
    conn = get_db_connection()
    try:
        run_count = get_run_count(table_name)

        # Check if we already have complete data (run 5+)
        if run_count >= 5:
            print(f"{table_name} already complete. Skipping collection.")
            return

        # Get current data count
        daily_table = "daily_" + table_name.replace("weekly_", "")
        current_count = conn.execute(f'SELECT COUNT(*) FROM "{daily_table}"').fetchone()[0] or 0

        # Process the data
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])

        # For runs 1-4, limit to next 25 rows after current count
        if run_count < 4:
            df = df.iloc[current_count:current_count + 25]

        # Insert new daily records
        for _, row in df.iterrows():
            conn.execute(f"""
            INSERT OR IGNORE INTO "{daily_table}" (date, cases)
            VALUES (?, ?)
            """, (row['date'].strftime('%Y-%m-%d'), row.get('cases')))

        # Update weekly aggregation
        weekly_data = conn.execute(f"""
        WITH daily_cases AS (
            SELECT
                date,
                cases - LAG(cases, 1) OVER (ORDER BY date) AS daily_cases
            FROM "{daily_table}"
            WHERE cases IS NOT NULL
            AND date BETWEEN ? AND ?
        )
        SELECT
            CAST(strftime('%Y', date) AS INTEGER) * 100 + CAST(strftime('%W', date) AS INTEGER) AS week_id,
            SUM(daily_cases) AS weekly_cases
        FROM daily_cases
        WHERE daily_cases IS NOT NULL
        GROUP BY week_id
        ORDER BY week_id
        """, (START_DATE.strftime('%Y-%m-%d'), END_DATE.strftime('%Y-%m-%d'))).fetchall()
        
        for week_id, weekly_cases in weekly_data:
            conn.execute(f"""
            INSERT OR REPLACE INTO "{table_name}" (week_id, weekly_cases)
            VALUES (?, ?)
            """, (week_id, weekly_cases))

        conn.commit()

        # Print status
        new_count = conn.execute(f'SELECT COUNT(*) FROM "{daily_table}"').fetchone()[0]
        weekly_count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        increment_run_count(table_name)
        current_run = run_count + 1
        print(f"COVID data processing complete for {table_name} (Run {current_run}):")
        print(f"- Previous daily records: {current_count}")
        print(f"- New daily records: {new_count}")
        print(f"- Records added this run: {new_count - current_count}")
        print(f"- Weekly aggregated data: {weekly_count} rows")

    except Exception as e:
        print(f"Error in store_covid_data: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def fetch_and_store_michigan_covid():
    api_key = get_api_key(COVID_API_KEY_ENV)
    url = f"{COVID_BASE_URL}/state/MI.timeseries.json?apiKey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json().get("actualsTimeseries", [])
        store_covid_data(data, "weekly_michigan_covid_data")

def fetch_and_store_national_covid():
    api_key = get_api_key(COVID_API_KEY_ENV)
    url = f"{COVID_BASE_URL}/country/US.timeseries.json?apiKey={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json().get("actualsTimeseries", [])
        store_covid_data(data, "weekly_national_covid_data")

def fetch_and_store_flu_data():
    try:
        conn = get_db_connection()
        run_count = get_run_count("flu_data_march_2020_to_2023")

        # Check if we already have complete data (run 5+)
        if run_count >= 5:
            print("Flu data already complete. Skipping collection.")
            return

        # Get current data count
        current_count = conn.execute("SELECT COUNT(*) FROM flu_data_march_2020_to_2023").fetchone()[0] or 0

        regions = {"mi": 1, "nat": 2}
        epiweeks = "202010-202310"
        base_url = "https://api.delphi.cmu.edu/epidata/fluview/"
        api_key = get_api_key(FLU_API_KEY_ENV)
        params = {"regions": ",".join(regions.keys()), "epiweeks": epiweeks, "auth": api_key}

        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if data["result"] == 1:
            df = pd.DataFrame(data["epidata"])
            df["region_key"] = df["region"].map(regions)
            df["date"] = df["epiweek"].apply(
                lambda ew: Week(int(str(ew)[:4]), int(str(ew)[4:])).startdate().strftime('%Y-%m-%d')
            )
            df["week_id"] = df["date"].apply(lambda d: get_week_id(d))

            # For runs 1-4, limit to next 25 rows after current count
            if run_count < 4:
                df = df.iloc[current_count:current_count + 25]

            # Insert new records
            for _, row in df.iterrows():
                conn.execute("""
                INSERT OR IGNORE INTO flu_data_march_2020_to_2023
                (region_key, date, week_id, num_ili)
                VALUES (?, ?, ?, ?)
                """, (row["region_key"], row["date"], row["week_id"], row["num_ili"]))

            conn.commit()
            
            # Print status
            new_count = conn.execute("SELECT COUNT(*) FROM flu_data_march_2020_to_2023").fetchone()[0]
            
            increment_run_count("flu_data_march_2020_to_2023")
            current_run = run_count + 1
            print(f"Flu data processing complete (Run {current_run}):")
            print(f"- Previous records: {current_count}")
            print(f"- New records: {new_count}")
            print(f"- Records added this run: {new_count - current_count}")
            
        else:
            print(f"API Error: {data['message']}")
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def collect_all_data():
    """Initialize database and collect all data"""
    # Create database and tables if they don't exist
    if not create_database():
        print("Failed to create database. Exiting.")
        return False
        
    # Initialize run counts table
    initialize_run_counts()
    
    # Collect data from all sources
    try:
        process_weather_data(MICHIGAN_LOCATION, START_DATE, END_DATE, "michigan_weather_data")
        fetch_and_store_michigan_covid()
        fetch_and_store_national_covid()
        fetch_and_store_flu_data()
        return True
    except Exception as e:
        print(f"Error collecting data: {str(e)}")
        return False

if __name__ == "__main__":
    collect_all_data()
