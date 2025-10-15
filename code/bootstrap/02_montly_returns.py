import sqlite3
import pandas as pd
import numpy as np

#Made by deepseek
#Creates the monthly returns from the "company_data_wide" table

def calculate_monthly_returns(db_file_path):
    """
    Calculate monthly stock returns on daily frequency using 20-day rolling windows
    With reverse chronological order (latest dates at the top)
    CORRECTED: Proper backward-looking returns in reverse chronological order
    """
    
    conn = sqlite3.connect(db_file_path)
    
    # Read the original price data
    price_df = pd.read_sql("SELECT * FROM company_data_wide", conn)
    
    print(f"Original data shape: {price_df.shape}")
    print(f"First few dates: {price_df['date'].head().tolist()}")
    print(f"Last few dates: {price_df['date'].tail().tolist()}")
    
    # Handle mixed date formats
    def parse_date(date_val):
        """Parse dates that could be either strings or numbers"""
        if isinstance(date_val, str) and '-' in date_val:
            # It's a proper date string like "2025-09-26 00:00:00"
            return pd.to_datetime(date_val, errors='coerce')
        else:
            # It's a number like 1255 - treat as sequential index
            return pd.NaT  # Mark as missing, we'll handle this
    
    # Apply date parsing
    price_df['parsed_date'] = price_df['date'].apply(parse_date)
    
    # Count how many dates were successfully parsed
    valid_dates = price_df['parsed_date'].notna().sum()
    print(f"Successfully parsed {valid_dates} dates out of {len(price_df)}")
    
    if valid_dates == 0:
        print("No valid dates found. Using sequential numbering.")
        # If no dates could be parsed, use the row number as index (latest first)
        price_df = price_df.sort_index(ascending=True)  # Keep original order for sequential
        price_df.set_index(pd.Index(range(len(price_df))), inplace=True)
    else:
        # Use the parsed dates, drop rows with invalid dates
        price_df = price_df[price_df['parsed_date'].notna()]
        price_df = price_df.sort_values('parsed_date', ascending=False)  # REVERSE CHRONOLOGICAL
        price_df.set_index('parsed_date', inplace=True)
        price_df.drop(columns=['date'], inplace=True)
        price_df.index.name = 'date'
    
    print(f"Final data shape: {price_df.shape}")
    print(f"Companies: {list(price_df.columns)}")
    print(f"Date range: {price_df.index.max()} to {price_df.index.min()}")  # Swapped min/max
    
    # Calculate monthly returns using 20-day rolling window
    monthly_returns = pd.DataFrame()
    
    for company in price_df.columns:
        # Get the price series for this company
        price_series = price_df[company].dropna()
        
        # Only calculate if we have enough data
        if len(price_series) > 20:
            # CORRECTED: In reverse chronological order, we need to look FORWARD 20 periods
            # to get the price from 20 trading days AGO (which appears later in the dataframe)
            log_prices = np.log(price_series)
            
            # Since we're in reverse order (latest first), shift(-20) looks FORWARD to older prices
            # This gives us: log(current_price) - log(price_20_days_ago)
            monthly_return = log_prices - log_prices.shift(-20)
            
            monthly_returns[company] = monthly_return
    
    # Reset index to have date as a column
    monthly_returns.reset_index(inplace=True)
    monthly_returns.rename(columns={'index': 'date'}, inplace=True)
    
    # Ensure sorting by date (latest first) - REVERSE CHRONOLOGICAL
    monthly_returns = monthly_returns.sort_values('date', ascending=False)
    
    # Save to new table in SQLite
    monthly_returns.to_sql('monthly_returns_daily_frequency', conn, if_exists='replace', index=False)
    
    print(f"\nMonthly returns table created!")
    print(f"Returns data shape: {monthly_returns.shape}")
    print(f"Latest date (first row): {monthly_returns['date'].iloc[0]}")
    print(f"Earliest date (last row): {monthly_returns['date'].iloc[-1]}")
    
    # Show some statistics
    returns_data = monthly_returns.iloc[:, 1:]  # Exclude date column
    valid_returns = returns_data.replace([np.inf, -np.inf], np.nan)
    
    print(f"\nReturn statistics:")
    print(f"Mean monthly return: {valid_returns.mean().mean():.6f}")
    print(f"Std of monthly returns: {valid_returns.std().mean():.6f}")
    
    # Show preview of the data (latest dates first)
    print(f"\nFirst 5 rows of monthly returns (latest dates first):")
    print(monthly_returns.head())
    
    conn.commit()
    conn.close()
    
    return monthly_returns

def clean_database_and_calculate(db_file_path):
    """
    Clean the database by removing rows with invalid dates, then calculate returns
    With reverse chronological order and CORRECTED return calculation
    """
    
    conn = sqlite3.connect(db_file_path)
    
    # First, let's see what's really in the date column
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, COUNT(*) 
        FROM company_data_wide 
        GROUP BY date 
        ORDER BY date DESC
        LIMIT 10
    """)
    date_samples = cursor.fetchall()
    
    print("Latest date samples from database:")
    for date, count in date_samples:
        print(f"  '{date}' (count: {count})")
    
    # Find the problematic rows
    cursor.execute("""
        SELECT rowid, date 
        FROM company_data_wide 
        WHERE date NOT LIKE '____-__-__ __:__:__'
        LIMIT 10
    """)
    problematic = cursor.fetchall()
    
    print(f"\nFound {len(problematic)} problematic date formats:")
    for rowid, date in problematic:
        print(f"  Row {rowid}: '{date}'")
    
    # Option 1: Delete problematic rows
    cursor.execute("""
        DELETE FROM company_data_wide 
        WHERE date NOT LIKE '____-__-__ __:__:__'
    """)
    deleted_count = cursor.rowcount
    print(f"Deleted {deleted_count} rows with invalid date formats")
    
    conn.commit()
    
    # Now read the cleaned data
    price_df = pd.read_sql("SELECT * FROM company_data_wide", conn)
    
    if len(price_df) == 0:
        print("No data left after cleaning! Restoring original approach.")
        conn.close()
        return calculate_monthly_returns_sequential(db_file_path)
    
    # Convert dates properly and sort by latest first
    price_df['date'] = pd.to_datetime(price_df['date'], format='%Y-%m-%d %H:%M:%S')
    price_df = price_df.sort_values('date', ascending=False)  # REVERSE CHRONOLOGICAL
    price_df.set_index('date', inplace=True)
    
    print(f"Cleaned data shape: {price_df.shape}")
    print(f"Date range: Latest = {price_df.index[0]}, Earliest = {price_df.index[-1]}")
    
    # Calculate returns
    monthly_returns = pd.DataFrame()
    
    for company in price_df.columns:
        price_series = price_df[company].dropna()
        
        if len(price_series) > 20:
            log_prices = np.log(price_series)
            
            # CORRECTED: Use shift(-20) to look forward to older prices in reverse chronological order
            monthly_return = log_prices - log_prices.shift(-20)
            monthly_returns[company] = monthly_return
    
    monthly_returns.reset_index(inplace=True)
    monthly_returns.rename(columns={'index': 'date'}, inplace=True)
    
    # Keep reverse chronological order
    monthly_returns = monthly_returns.sort_values('date', ascending=False)
    
    # Save to database
    monthly_returns.to_sql('monthly_returns_daily_frequency', conn, if_exists='replace', index=False)
    
    print(f"\nSuccess! Monthly returns calculated from cleaned data.")
    print(f"Returns table shape: {monthly_returns.shape}")
    print(f"Latest date: {monthly_returns['date'].iloc[0]}")
    print(f"Earliest date: {monthly_returns['date'].iloc[-1]}")
    
    conn.commit()
    conn.close()
    
    return monthly_returns

def calculate_monthly_returns_sequential(db_file_path):
    """
    Use sequential numbering when dates are invalid, with latest first
    CORRECTED: Proper backward-looking returns
    """
    
    conn = sqlite3.connect(db_file_path)
    
    # Read data without date conversion
    price_df = pd.read_sql("SELECT * FROM company_data_wide", conn)
    
    # Drop the date column and use sequential index (latest first)
    price_df = price_df.drop(columns=['date'])
    price_df = price_df.sort_index(ascending=True).reset_index(drop=True)  # Keep original order
    
    print(f"Using sequential numbering for {len(price_df)} trading days (latest first)")
    print(f"Companies: {list(price_df.columns)}")
    
    # Calculate returns
    monthly_returns = pd.DataFrame()
    
    for company in price_df.columns:
        price_series = price_df[company].dropna()
        
        if len(price_series) > 20:
            log_prices = np.log(price_series)
            
            # CORRECTED: In sequential order with latest first, shift(-20) looks forward to older data
            monthly_return = log_prices - log_prices.shift(-20)
            monthly_returns[company] = monthly_return
    
    # Add sequential dates (day numbers) with latest first
    monthly_returns['trading_day'] = range(len(monthly_returns))
    monthly_returns = monthly_returns[['trading_day'] + [col for col in monthly_returns.columns if col != 'trading_day']]
    
    # Save to database
    monthly_returns.to_sql('monthly_returns_daily_frequency', conn, if_exists='replace', index=False)
    
    print(f"\nMonthly returns calculated using sequential day numbering (latest first)")
    print(f"Returns table shape: {monthly_returns.shape}")
    
    conn.commit()
    conn.close()
    
    return monthly_returns

# Quick function to verify the returns calculation
def verify_calculation(db_file_path):
    """
    Verify that the monthly returns are calculated correctly
    """
    conn = sqlite3.connect(db_file_path)
    
    try:
        # Get a sample of returns data
        returns_df = pd.read_sql("""
            SELECT * FROM monthly_returns_daily_frequency 
            ORDER BY date DESC 
            LIMIT 30
        """, conn)
        
        print("=== VERIFYING CALCULATION ===")
        print("Latest 30 rows (should have NaN for first 20 rows due to lookback):")
        
        # Check how many NaN values in the first rows
        first_company = returns_df.columns[1]  # First company column
        nan_count_first_20 = returns_df[first_company].head(20).isna().sum()
        print(f"NaN values in first 20 rows for {first_company}: {nan_count_first_20}/20")
        
        # Show some actual calculations
        print(f"\nSample calculation verification:")
        for i in range(20, min(25, len(returns_df))):
            date = returns_df.iloc[i]['date']
            return_val = returns_df.iloc[i][first_company]
            if not pd.isna(return_val):
                print(f"Row {i}: {date} - Return: {return_val:.6f}")
        
    except Exception as e:
        print(f"Could not verify calculation: {e}")
    
    conn.close()

# Usage
if __name__ == "__main__":
    db_file = "companies_wide.db"
    
    print("=== CLEANING DATABASE AND CALCULATING RETURNS ===")
    print("=== REVERSE CHRONOLOGICAL ORDER (LATEST FIRST) ===\n")
    print("=== CORRECTED: Using shift(-20) for backward-looking returns ===\n")
    
    # Try cleaning the database first
    returns_df = clean_database_and_calculate(db_file)
    
    print(f"\n=== RESULTS ===")
    print(f"First 5 rows (latest dates first):")
    print(returns_df.head())
    
    print(f"\nLast 5 rows (earliest dates):")
    print(returns_df.tail())
    
    # Verify the calculation
    verify_calculation(db_file)