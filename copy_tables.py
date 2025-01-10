import os
import sqlite3
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Initialize Supabase client
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# SQLite database path
sqlite_db_path = "data/processed_reports_with_types.db"

# Create the update_logs folder if it doesn't exist
log_folder = "update_logs"
os.makedirs(log_folder, exist_ok=True)

# Generate a timestamped log file name
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_path = os.path.join(log_folder, f"update_log_{timestamp}.txt")

# Function to convert Unix timestamps to readable date format
def convert_dates(data, col_names):
    for row in data:
        for col_name in col_names:
            if "date" in col_name.lower() and row[col_name] is not None:
                try:
                    # Convert Unix timestamp to a timezone-aware datetime object in UTC
                    row[col_name] = datetime.fromtimestamp(row[col_name], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    row[col_name] = None  # Handle invalid timestamps
    return data

# Function to copy multiple tables to Supabase in chunks
def copy_tables_to_supabase(tables, chunk_size=1000):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(sqlite_db_path)

        # Handle non-UTF-8 text by ignoring invalid characters
        conn.text_factory = lambda x: str(x, "utf-8", "replace")
        cursor = conn.cursor()

        # Open a log file to record success/failure
        with open(log_file_path, "w") as log_file:
            for table in tables:
                log_file.write(f"\nCopying table: {table}\n")
                print(f"Copying table: {table}")

                # Select all data from the current table
                cursor.execute(f"SELECT * FROM {table};")
                rows = cursor.fetchall()

                # Get column names
                col_names = [description[0] for description in cursor.description]

                # Process in chunks
                for i in range(0, len(rows), chunk_size):
                    chunk = rows[i:i + chunk_size]
                    data_to_insert = [dict(zip(col_names, row)) for row in chunk]

                    # Convert date columns
                    data_to_insert = convert_dates(data_to_insert, col_names)

                    try:
                        # Insert chunk into Supabase
                        supabase.table(table).insert(data_to_insert).execute()
                        message = f"Chunk {i // chunk_size + 1} for table {table}: Success\n"
                        log_file.write(message)
                        print(message)
                    except Exception as e:
                        message = f"Chunk {i // chunk_size + 1} for table {table}: Failed - {str(e)}\n"
                        log_file.write(message)
                        print(message)
                        break

        # Close SQLite connection
        conn.close()
        print(f"Data upload completed. Check '{log_file_path}' for details.")

    except Exception as e:
        print("Error:", e)

