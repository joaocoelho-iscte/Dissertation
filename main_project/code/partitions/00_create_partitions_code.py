import sqlite3
import time


#Code from (Insert book given by the teacher) that was originaly compiled in fortran for 20 partitions. 
#This code has been modified by deepseek to be in python and to give 80 partitions.

def generate_n80_partitions_sqlite_only():
    """
    Generate partitions for N=80 and store ONLY in SQLite database
    """
    start_time = time.time()
    
    # Create SQLite database
    conn = sqlite3.connect('n80_partitions.db')
    cursor = conn.cursor()
    
    # Create partitions table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS partitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            partition_text TEXT,
            part_count INTEGER,
            largest_part INTEGER
        )
    ''')
    
    # Initialize partition array
    IP = [80] + [0] * 79
    
    # Counter for progress tracking
    partition_count = 0
    last_print_time = time.time()
    
    # Write initial partition
    non_zero_parts = [val for val in IP if val > 0]
    partition_str = " ".join(str(val) for val in non_zero_parts)
    part_count = len(non_zero_parts)
    largest_part = max(non_zero_parts)
    
    cursor.execute('''
        INSERT INTO partitions (partition_text, part_count, largest_part)
        VALUES (?, ?, ?)
    ''', (partition_str, part_count, largest_part))
    partition_count += 1
    
    print("Starting N=80 partition generation...")
    print("Estimated total partitions: ~15,796,476")
    
    # Main generation loop
    while True:
        # Find rightmost part > 1
        J = 0
        while J < 80 and IP[J] > 1:
            J += 1
        
        # If all parts are 1, we're done
        if J == 0:
            break
        
        # The part to modify
        part_to_modify = J - 1
        
        # Calculate remaining sum
        sum_before = sum(IP[:part_to_modify])
        remaining = 80 - sum_before
        
        current_value = IP[part_to_modify]
        new_value = current_value - 1
        
        count = remaining // new_value
        remainder = remaining % new_value
        
        # Update partition
        for i in range(count):
            if part_to_modify + i < 80:
                IP[part_to_modify + i] = new_value
        
        if part_to_modify + count < 80:
            IP[part_to_modify + count] = remainder
        
        # Zero out remaining positions
        next_position = part_to_modify + count + (1 if remainder > 0 else 0)
        for i in range(next_position, 80):
            IP[i] = 0
        
        # Write partition to SQLite
        non_zero_parts = [val for val in IP if val > 0]
        partition_str = " ".join(str(val) for val in non_zero_parts)
        part_count = len(non_zero_parts)
        largest_part = max(non_zero_parts)
        
        cursor.execute('''
            INSERT INTO partitions (partition_text, part_count, largest_part)
            VALUES (?, ?, ?)
        ''', (partition_str, part_count, largest_part))
        partition_count += 1
        
        # Progress reporting
        current_time = time.time()
        if current_time - last_print_time >= 10:  # Print every 10 seconds
            elapsed = current_time - start_time
            partitions_per_second = partition_count / elapsed
            estimated_total_time = 15796476 / partitions_per_second if partitions_per_second > 0 else 0
            remaining_time = estimated_total_time - elapsed
            
            print(f"Progress: {partition_count:,} partitions generated "
                  f"({partitions_per_second:.1f}/sec) "
                  f"ETA: {remaining_time/60:.1f} minutes")
            last_print_time = current_time
            
            # Commit periodically to save progress
            if partition_count % 100000 == 0:
                conn.commit()
                print("Committed progress to database")
    
    # Final commit and cleanup
    conn.commit()
    conn.close()
    
    total_time = time.time() - start_time
    print(f"\nCompleted!")
    print(f"Total partitions generated: {partition_count:,}")
    print(f"Total time: {total_time/60:.2f} minutes")
    print(f"Partitions per second: {partition_count/total_time:.2f}")
    print(f"Database: n80_partitions.db")

def verify_database():
    """Verify the database contents"""
    conn = sqlite3.connect('n80_partitions.db')
    cursor = conn.cursor()
    
    # Count total partitions
    cursor.execute('SELECT COUNT(*) FROM partitions')
    total = cursor.fetchone()[0]
    print(f"Total partitions in database: {total:,}")
    
    # Check first and last partitions
    cursor.execute('SELECT * FROM partitions ORDER BY id LIMIT 1')
    first = cursor.fetchone()
    print(f"First partition: ID={first[0]}, {first[1]}")
    
    cursor.execute('SELECT * FROM partitions ORDER BY id DESC LIMIT 1')
    last = cursor.fetchone()
    print(f"Last partition: ID={last[0]}, {last[1]}")
    
    # Partition count distribution
    cursor.execute('''
        SELECT part_count, COUNT(*) 
        FROM partitions 
        GROUP BY part_count 
        ORDER BY part_count
        LIMIT 10
    ''')
    print("\nPartition count distribution (first 10):")
    for part_count, count in cursor:
        print(f"  {part_count} parts: {count:,} partitions")
    
    conn.close()

if __name__ == "__main__":
    generate_n80_partitions_sqlite_only()
    print("\n" + "="*50)
    verify_database()