import sqlite3

#Code to check the schema/headers of the SQLite database created (n80.partitions.db)

def check_database_headers():
    """Check the schema/headers of the SQLite database"""
    
    # Connect to the database
    conn = sqlite3.connect('n80_partitions.db')
    cursor = conn.cursor()
    
    print("=== DATABASE SCHEMA ===")
    
    # Get all table names in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    print(f"Tables in database: {[table[0] for table in tables]}")
    print()
    
    # Check schema for each table
    for table in tables:
        table_name = table[0]
        print(f"Table: {table_name}")
        print("-" * 40)
        
        # Get the schema (column definitions) for this table
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # Print column headers with details
        print(f"{'Column Name':<15} {'Data Type':<12} {'Nullable':<8} {'Primary Key'}")
        print("-" * 50)
        for column in columns:
            # column format: (cid, name, type, notnull, dflt_value, pk)
            col_name = column[1]
            col_type = column[2]
            nullable = "YES" if column[3] == 0 else "NO"
            primary_key = "YES" if column[5] == 1 else "NO"
            
            print(f"{col_name:<15} {col_type:<12} {nullable:<8} {primary_key}")
        print()
    
    # Close connection
    conn.close()

def check_sample_data():
    """Check sample data to understand the structure"""
    
    conn = sqlite3.connect('n80_partitions.db')
    cursor = conn.cursor()
    
    print("=== SAMPLE DATA ===")
    
    # Get first 5 rows to see the data structure
    cursor.execute('SELECT * FROM partitions LIMIT 5')
    rows = cursor.fetchall()
    
    # Get column names
    cursor.execute('SELECT * FROM partitions LIMIT 0')  # Returns no rows, just headers
    column_names = [description[0] for description in cursor.description]
    
    print("Column names:", column_names)
    print("\nFirst 5 rows:")
    for i, row in enumerate(rows, 1):
        print(f"Row {i}: {row}")
    
    conn.close()

def check_database_stats():
    """Get some basic statistics about the database"""
    
    conn = sqlite3.connect('n80_partitions.db')
    cursor = conn.cursor()
    
    print("=== DATABASE STATISTICS ===")
    
    # Count total rows
    cursor.execute('SELECT COUNT(*) FROM partitions')
    total_rows = cursor.fetchone()[0]
    print(f"Total partitions: {total_rows:,}")
    
    # Check database file size
    import os
    if os.path.exists('n80_partitions.db'):
        file_size = os.path.getsize('n80_partitions.db')
        print(f"Database file size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
    
    # Get min and max IDs
    cursor.execute('SELECT MIN(id), MAX(id) FROM partitions')
    min_id, max_id = cursor.fetchone()
    print(f"ID range: {min_id} to {max_id}")
    
    conn.close()

# Run all checks
if __name__ == "__main__":
    check_database_headers()
    print("\n")
    check_sample_data()
    print("\n")
    check_database_stats()