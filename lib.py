import sqlite3
from typing import List, Tuple

def get_db_schema_sql(db_path: str) -> str:
    """
    Returns the SQL statements used to create the entire SQLite database schema.

    Args:
        db_path (str): The path to the SQLite database file.

    Returns:
        str: The SQL statements used to create the database schema.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query the sqlite_master table to get the SQL create statements
    cursor.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL;")
    create_statements = cursor.fetchall()

    conn.close()

    # Join the SQL statements with a newline for readability
    return "\n\n".join(statement[0] for statement in create_statements)


import sqlite3
from typing import List, Tuple

def query_db(db_path: str, query: str) -> List[Tuple]:
    """
    Executes a custom SQL query on a SQLite database, handling non-UTF-8 text.

    Args:
        db_path (str): The path to the SQLite database file.
        query (str): The SQL query to be executed.

    Returns:
        List[Tuple]: The query results as a list of tuples.
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)

        # Handle non-UTF-8 text by ignoring invalid characters
        conn.text_factory = lambda x: str(x, "utf-8", "replace")
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch all results
        results = cursor.fetchall()

        # Commit the transaction if needed
        conn.commit()

        # Close the connection
        conn.close()

        return results

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []

    except Exception as e:
        print(f"Error: {e}")
        return []

