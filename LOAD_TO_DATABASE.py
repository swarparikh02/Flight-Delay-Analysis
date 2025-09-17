import pandas as pd
import pyodbc
import os

# ✅ Configurable parameters
AIRLINE_CSV = r"D:\\Projects\\ADT_PROJECT\\2023\\final_airlines.csv"
AIRPORT_CSV = r"D:\\Projects\\ADT_PROJECT\\2023\\final_airports.csv"
FLIGHT_CSV = r"D:\\Projects\\ADT_PROJECT\\2023\\flights_cleaned_2023.csv"
TARGET_DATABASE = "flight_data_2023"
FLIGHT_ROW_LIMIT = 1450000
BATCH_SIZE = 10000  # Adjustable batch size for executemany

# ✅ Step 1: Connect to SQL Server
print("🔄 Connecting to SQL Server...")
conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=localhost\SQLEXPRESS;"
    f"Database={TARGET_DATABASE};"
    r"Trusted_Connection=yes;"
)
try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.fast_executemany = True  # Optimize executemany
    print("✅ Connected to SQL Server.")
except pyodbc.Error as e:
    print(f"❌ Failed to connect: {e}")
    exit(1)

# ✅ Step 2: Load CSVs
print("🔄 Loading CSV files...")
try:
    airline_df = pd.read_csv(AIRLINE_CSV)
    airport_df = pd.read_csv(AIRPORT_CSV)
    flight_df = pd.read_csv(FLIGHT_CSV, nrows=FLIGHT_ROW_LIMIT)
    print(f"✅ CSV files loaded. Limiting flight rows to first {FLIGHT_ROW_LIMIT}")
    print(f"Flight CSV columns: {flight_df.columns.tolist()}")
except Exception as e:
    print(f"❌ Failed to load CSVs: {e}")
    cursor.close()
    conn.close()
    exit(1)

# ✅ Step 3: Insert into AIRLINE table
print("🔄 Inserting into AIRLINE table...")
try:
    data = [tuple(row) for row in airline_df[['IATA_CODE', 'AIRLINE']].values]
    cursor.executemany(
        "INSERT INTO AIRLINE (IATA_CODE, AIRLINE) VALUES (?, ?)",
        data
    )
    conn.commit()
    print(f"✅ Inserted {len(data)} airline records.")
except pyodbc.Error as e:
    print(f"❌ Error inserting into AIRLINE: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ✅ Step 4: Insert into AIRPORT table
print("🔄 Inserting into AIRPORT table...")
try:
    data = [tuple(row) for row in airport_df[['IATA_CODE', 'AIRPORT', 'CITY', 'STATE']].values]
    cursor.executemany(
        "INSERT INTO AIRPORT (IATA_CODE, AIRPORT, CITY, STATE) VALUES (?, ?, ?, ?)",
        data
    )
    conn.commit()
    print(f"✅ Inserted {len(data)} airport records.")
except pyodbc.Error as e:
    print(f"❌ Error inserting into AIRPORT: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ✅ Step 5: Pre-validate FKs for FLIGHT table
print("🔄 Validating foreign keys for FLIGHT table...")
try:
    # Fetch valid IATA_CODEs from AIRLINE and AIRPORT tables
    cursor.execute("SELECT IATA_CODE FROM AIRLINE")
    valid_airlines = set(row[0] for row in cursor.fetchall())
    cursor.execute("SELECT IATA_CODE FROM AIRPORT")
    valid_airports = set(row[0] for row in cursor.fetchall())

    # Filter flight_df for valid FKs and skip problematic rows
    flight_df = flight_df[
        (flight_df['AIRLINE'].isin(valid_airlines)) &
        (flight_df['ORIGIN_AIRPORT'].isin(valid_airports)) &
        (flight_df['DESTINATION_AIRPORT'].isin(valid_airports)) &
        (~flight_df.index.isin(range(218995, 219006)))
    ]
    print(f"✅ Validated FKs. {len(flight_df)} valid flight rows after filtering.")
except pyodbc.Error as e:
    print(f"❌ Error validating FKs: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ✅ Step 6: Insert into FLIGHT table using executemany
print("🔄 Inserting into FLIGHT table...")
try:
    # Verify expected columns
    expected_columns = [
        'YEAR', 'MONTH', 'DAY', 'AIRLINE', 'TAIL_NUMBER', 'ORIGIN_AIRPORT',
        'DESTINATION_AIRPORT', 'DISTANCE', 'ARRIVAL_DELAY', 'DEPARTURE_DELAY',
        'DEPARTURE_TIME', 'CANCELLED', 'CANCELLATION_REASON'
    ]
    if not all(col in flight_df.columns for col in expected_columns):
        missing = [col for col in expected_columns if col not in flight_df.columns]
        raise ValueError(f"Missing columns in flight_df: {missing}")

    # Select only expected columns
    flight_df = flight_df[expected_columns]

    # Validate DEPARTURE_TIME format (HH:MM:SS)
    invalid_times = flight_df[~flight_df['DEPARTURE_TIME'].str.match(r'^\d{2}:\d{2}:\d{2}$', na=False)]
    if not invalid_times.empty:
        print(f"Warning: {len(invalid_times)} rows have invalid DEPARTURE_TIME values:")
        print(invalid_times['DEPARTURE_TIME'].unique()[:10])
        # Optionally set invalid values to None if column allows NULL
        flight_df.loc[~flight_df['DEPARTURE_TIME'].str.match(r'^\d{2}:\d{2}:\d{2}$', na=False), 'DEPARTURE_TIME'] = None

    # Prepare data as list of tuples
    data = [tuple(row) for row in flight_df.values]

    # Insert in batches
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        cursor.executemany(
            """
            INSERT INTO FLIGHT (
                YEAR, MONTH, DAY, AIRLINE, TAIL_NUMBER, ORIGIN_AIRPORT,
                DESTINATION_AIRPORT, DISTANCE, ARRIVAL_DELAY, DEPARTURE_DELAY,
                DEPARTURE_TIME, CANCELLED, CANCELLATION_REASON
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch
        )
        conn.commit()
        print(f"✅ Inserted batch {i // BATCH_SIZE + 1} of {len(data) // BATCH_SIZE + 1} ({len(batch)} rows)")

    print(f"✅ Inserted {len(data)} flight records using executemany.")
except pyodbc.Error as e:
    print(f"❌ Error inserting into FLIGHT: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)
except Exception as e:
    print(f"❌ Error preparing data for FLIGHT insert: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    exit(1)

# ✅ Step 7: Commit and close
print("🔄 Committing changes...")
conn.commit()
cursor.close()
conn.close()
print("✅ All data inserted and connection closed.")