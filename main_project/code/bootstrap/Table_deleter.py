import sqlite3

def delete_table(db_file_path, table_name):
    """
    Delete a table from SQLite database
    """
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print(f"Table '{table_name}' deleted successfully!")
    except Exception as e:
        print(f"Error deleting table: {e}")
    finally:
        conn.close()

# Usage
#for i in range(1,3):
#    delete_table("companies_wide.db", f"portfolio_simulation_{i}")

delete_table("companies_wide.db", "portfolio_20250902")