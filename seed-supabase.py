import os
import sqlite3
from dotenv import load_dotenv
from supabase import create_client, Client
from lib import query_db

# Load environment variables from .env file
load_dotenv()

# Initialize Supabase client
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

# SQLite database path
sqlite_db_path = "data/processed_reports_with_types.db"

# Function to copy the parcels table in chunks
def copy_parcels_to_supabase(chunk_size=1000):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(sqlite_db_path)

        # Handle non-UTF-8 text by ignoring invalid characters
        conn.text_factory = lambda x: str(x, "utf-8", "replace")
        cursor = conn.cursor()

        # Open a log file to record success/failure
        with open("upload_log.txt", "w") as log_file:
            # Select all data from parcels table
            cursor.execute("SELECT * FROM parcels;")
            rows = cursor.fetchall()

            # Get column names
            col_names = [description[0] for description in cursor.description]

            # Process in chunks
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                data_to_insert = [dict(zip(col_names, row)) for row in chunk]
                # print(data_to_insert) 
                # break
                try:
                    # Insert chunk into Supabase
                    supabase.table("parcels").insert(data_to_insert).execute()
                    message = f"Chunk {i // chunk_size + 1}: Success\n"
                    log_file.write(message)
                    print(message)


                except Exception as e:
                    message = f"Chunk {i // chunk_size + 1}: Failed - {str(e)}\n"
                    log_file.write(message)
                    print(message)

        # Close SQLite connection
        conn.close()
        print("Data upload completed. Check 'upload_log.txt' for details.")

    except Exception as e:
        print("Error:", e)

# Run the function
copy_parcels_to_supabase(chunk_size=1000)
