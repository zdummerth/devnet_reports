import sqlite3

def search_large_value(db_path, value):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Execute the query to find the large value
        query = "SELECT * FROM sales WHERE net_selling_price = ?;"
        cursor.execute(query, (value,))
        results = cursor.fetchall()

        # Print the results
        if results:
            print("Found matching row(s):")
            for row in results:
                print(row)
        else:
            print("No matching row found.")

        # Close the connection
        conn.close()

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")

# Run the function
search_large_value("data/processed_reports_with_types.db", "11068113.23")
