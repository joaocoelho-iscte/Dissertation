import sqlite3
import pandas as pd

#Code to randomly select a sample of 100,000 portfolios from (n80_partitions.db)

def random_sample_sqlite():
    """Randomly sample 100,000 portfolios directly from SQLite"""
    
    conn = sqlite3.connect('n80_partitions.db')
    
    # Method 1A: Using SQL RANDOM() with LIMIT
    print("Method 1A: SQL RANDOM() sampling...")
    query = """
        SELECT * FROM partitions 
        ORDER BY RANDOM() 
        LIMIT 100000
    """
    
    sampled_df = pd.read_sql_query(query, conn)
    print(f"Sampled {len(sampled_df)} portfolios")
    
    # Save to new table
    sampled_df.to_sql('sampled_partitions', conn, if_exists='replace', index=False)
    
    # Verify
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM sampled_partitions')
    count = cursor.fetchone()[0]
    print(f"Verified: {count} portfolios in sampled table")
    
    conn.close()
    return sampled_df

# Run the sampling
sampled_data = random_sample_sqlite()