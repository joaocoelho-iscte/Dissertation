import sqlite3
import csv

def main():
    # Initialize a list of 80 zeros to store partitions
    IP = [0] * 80
    
    # Create SQLite database and table
    conn = sqlite3.connect('partitions.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS partitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number INTEGER,
            partition_text TEXT,
            partition_json TEXT,
            part_count INTEGER
        )
    ''')
    
    # Create CSV file
    with open('partitions.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write CSV header
        csv_writer.writerow(['number', 'partition', 'part_count'] + [f'part_{i+1}' for i in range(80)])
        
        # Loop through numbers 1 to 80
        for N in range(1, 81):
            print(f"Processing number: {N}")
            
            # Initialize partition: first element is N, rest are zeros
            IP = [N] + [0] * 79
            
            # Write initial partition to both files
            partition_str = " ".join(str(val) for val in IP if val > 0)
            non_zero_parts = [val for val in IP if val > 0]
            part_count = len(non_zero_parts)
            
            # Write to CSV
            csv_writer.writerow([N, partition_str, part_count] + IP)
            
            # Write to SQLite
            cursor.execute('''
                INSERT INTO partitions (number, partition_text, partition_json, part_count)
                VALUES (?, ?, ?, ?)
            ''', (N, partition_str, str(IP), part_count))
            
            # Main loop to generate all partitions for current N
            while True:
                # Find the rightmost part that is greater than 1
                J = 0
                while J < 80 and IP[J] > 1:
                    J += 1
                
                # If J == 0, all parts are 1, we're done
                if J == 0:
                    break
                
                # The part to modify is at position J-1
                part_to_modify = J - 1
                
                # Calculate sum of all parts before the one we're modifying
                sum_before = sum(IP[:part_to_modify])
                remaining = N - sum_before
                
                current_value = IP[part_to_modify]
                new_value = current_value - 1
                
                count = remaining // new_value
                remainder = remaining % new_value
                
                # Update the partition
                for i in range(count):
                    if part_to_modify + i < 80:
                        IP[part_to_modify + i] = new_value
                
                if part_to_modify + count < 80:
                    IP[part_to_modify + count] = remainder
                
                # Zero out remaining positions
                next_position = part_to_modify + count + (1 if remainder > 0 else 0)
                for i in range(next_position, 80):
                    IP[i] = 0
                
                # Write this partition to both files
                partition_str = " ".join(str(val) for val in IP if val > 0)
                non_zero_parts = [val for val in IP if val > 0]
                part_count = len(non_zero_parts)
                
                # Write to CSV
                csv_writer.writerow([N, partition_str, part_count] + IP)
                
                # Write to SQLite
                cursor.execute('''
                    INSERT INTO partitions (number, partition_text, partition_json, part_count)
                    VALUES (?, ?, ?, ?)
                ''', (N, partition_str, str(IP), part_count))
            
            # Commit after each number to save progress
            conn.commit()
            print(f"Completed number: {N}")
    
    # Close database connection
    conn.close()
    print("Program completed - data saved to partitions.csv and partitions.db")

def create_advanced_schema():
    """Create a more advanced database schema with additional useful fields"""
    conn = sqlite3.connect('partitions_advanced.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS partitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            number INTEGER,
            partition_text TEXT,
            partition_array TEXT,
            part_count INTEGER,
            largest_part INTEGER,
            smallest_part INTEGER,
            is_primary_partition BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create index for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_number ON partitions(number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_part_count ON partitions(part_count)')
    
    conn.commit()
    conn.close()

def advanced_main():
    """Advanced version with more detailed data storage"""
    create_advanced_schema()
    
    conn = sqlite3.connect('partitions_advanced.db')
    cursor = conn.cursor()
    
    # Create detailed CSV
    with open('partitions_detailed.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        # Write detailed CSV header
        header = ['number', 'partition', 'part_count', 'largest_part', 'smallest_part', 'is_primary']
        header.extend([f'part_{i+1}' for i in range(80)])
        csv_writer.writerow(header)
        
        IP = [0] * 80
        
        for N in range(1, 81):
            print(f"Processing number: {N}")
            
            IP = [N] + [0] * 79
            
            # Process initial partition
            write_partition_data(N, IP, csv_writer, cursor, is_primary=True)
            
            while True:
                J = 0
                while J < 80 and IP[J] > 1:
                    J += 1
                
                if J == 0:
                    break
                
                part_to_modify = J - 1
                sum_before = sum(IP[:part_to_modify])
                remaining = N - sum_before
                current_value = IP[part_to_modify]
                new_value = current_value - 1
                count = remaining // new_value
                remainder = remaining % new_value
                
                for i in range(count):
                    if part_to_modify + i < 80:
                        IP[part_to_modify + i] = new_value
                
                if part_to_modify + count < 80:
                    IP[part_to_modify + count] = remainder
                
                next_position = part_to_modify + count + (1 if remainder > 0 else 0)
                for i in range(next_position, 80):
                    IP[i] = 0
                
                write_partition_data(N, IP, csv_writer, cursor, is_primary=False)
            
            conn.commit()
            print(f"Completed number: {N}")
    
    conn.close()
    print("Advanced program completed")

def write_partition_data(N, IP, csv_writer, cursor, is_primary):
    """Helper function to write partition data to both CSV and SQLite"""
    non_zero_parts = [val for val in IP if val > 0]
    partition_str = " ".join(str(val) for val in non_zero_parts)
    part_count = len(non_zero_parts)
    largest_part = max(non_zero_parts) if non_zero_parts else 0
    smallest_part = min(non_zero_parts) if non_zero_parts else 0
    
    # Write to CSV
    csv_row = [N, partition_str, part_count, largest_part, smallest_part, is_primary]
    csv_row.extend(IP)
    csv_writer.writerow(csv_row)
    
    # Write to SQLite
    cursor.execute('''
        INSERT INTO partitions 
        (number, partition_text, partition_array, part_count, largest_part, smallest_part, is_primary_partition)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (N, partition_str, str(IP), part_count, largest_part, smallest_part, is_primary))

if __name__ == "__main__":
    # Run basic version
    main()
    
    # Uncomment below to run advanced version
    # advanced_main()