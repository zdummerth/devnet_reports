from lib import get_db_schema_sql, query_db
from copy_tables import copy_tables_to_supabase

# Path to your SQLite database
# db_path = "data/processed_reports_with_types.db"

# results = query_db(db_path, "select parcel_number from parcels limit 1;")
# print(results)
# Get the schema as SQL
# schema_sql = get_db_schema_sql(db_path)

# # Print the schema
# print("Database Schema:\n")
# print(schema_sql)

# Run the function with the list of tables
tables_to_copy = ["report_info", "report_dates"]
copy_tables_to_supabase(tables_to_copy, chunk_size=5000)