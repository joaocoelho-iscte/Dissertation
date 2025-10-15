import sqlite3
import pandas as pd
import glob
import os

#made with deepseek
#takes the csv files from ./data/clean and creates a sqlite3 file named "companies_wide.db"
#It just takes the first two column (date and closing price)

def create_company_wide_table(csv_folder_path, db_file_path):
    """
    Create a wide table with date column and one column per company
    containing data from the second column of each CSV file
    """
    
    conn = sqlite3.connect(db_file_path)
    
    # Dictionary to store all company data
    all_data = {}
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))
    
    if not csv_files:
        print("No CSV files found in the specified directory.")
        return
    
    print(f"Found {len(csv_files)} CSV files")
    
    # First pass: collect all unique dates and company data
    for csv_file in csv_files:
        try:
            # Get company name from filename (without extension)
            company_name = os.path.splitext(os.path.basename(csv_file))[0]
            
            # Read first two columns
            df = pd.read_csv(csv_file, usecols=[0, 1])
            df.columns = ['date', 'value']
            
            # Store in dictionary with company name as key
            all_data[company_name] = df.set_index('date')['value']
            
            print(f"Loaded data for: {company_name}")
            
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
    
    if not all_data:
        print("No data was successfully loaded.")
        return
    
    # Combine all data into a single DataFrame
    combined_df = pd.concat(all_data, axis=1)
    combined_df.reset_index(inplace=True)
    combined_df.rename(columns={'index': 'date'}, inplace=True)
    
    # Save to SQLite
    combined_df.to_sql('company_data_wide', conn, if_exists='replace', index=False)
    
    print(f"\nCreated table with columns: {list(combined_df.columns)}")
    print(f"Total rows: {len(combined_df)}")
    
    # Show preview
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM company_data_wide LIMIT 5")
    rows = cursor.fetchall()
    
    print("\nFirst 5 rows:")
    for row in rows:
        print(row)
    
    conn.commit()
    conn.close()

# Usage
if __name__ == "__main__":
    csv_folder = "./data/clean"  # Folder with your CSV files
    db_file = "companies_wide.db"  # Output database
    
    create_company_wide_table(csv_folder, db_file)