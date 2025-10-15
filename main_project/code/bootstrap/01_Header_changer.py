import sqlite3
import pandas as pd

#Made by deepseek
#Changes the headers of the "company_data_wide" table

def quick_clean_names(db_file_path):
    """
    Quick function to clean company names only
    """
    conn = sqlite3.connect(db_file_path)
    
    # Read data
    df = pd.read_sql("SELECT * FROM company_data_wide", conn)
    
    # Clean column names
    new_columns = []
    for col in df.columns:
        if col == 'date':
            new_columns.append(col)
        else:
            # Remove the prefix
            cleaned = col.replace('HistoricalDataStocks20250926_', '')
            new_columns.append(cleaned)
    
    df.columns = new_columns
    
    # Save back
    df.to_sql('company_data_wide', conn, if_exists='replace', index=False)
    
    print("Company names cleaned!")
    print("New column names:", list(df.columns))
    
    conn.commit()
    conn.close()

# Run the function
if __name__ == "__main__":
    quick_clean_names("companies_wide.db")