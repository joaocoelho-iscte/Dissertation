import sqlite3
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import random
from datetime import datetime, timedelta

class PortfolioSimulationData:
    """
    Class to prepare data for portfolio simulations by selecting subsets of stocks
    and their historical returns
    """
    
    def __init__(self, db_file_path: str):
        self.db_file_path = db_file_path
        self.returns_data = None
        self.load_data()
    
    def load_data(self) -> pd.DataFrame:
        """Load monthly returns data from database"""
        conn = sqlite3.connect(self.db_file_path)
        self.returns_data = pd.read_sql("SELECT * FROM monthly_returns_daily_frequency", conn)
        self.returns_data['date'] = pd.to_datetime(self.returns_data['date'])
        self.returns_data = self.returns_data.set_index('date')
        self.returns_data = self.returns_data.sort_index(ascending=True)  # OLDEST FIRST for correct windowing
        conn.close()
        
        print(f"Loaded returns data: {self.returns_data.shape}")
        print(f"Date range: {self.returns_data.index.min()} to {self.returns_data.index.max()}")
        print(f"Total trading days: {len(self.returns_data)}")
        return self.returns_data
    
    def get_available_stocks(self, min_data_points: int = 250) -> List[str]:
        """
        Get list of stocks with sufficient data
        """
        available_stocks = []
        for column in self.returns_data.columns:
            non_na_count = self.returns_data[column].notna().sum()
            if non_na_count >= min_data_points:
                available_stocks.append(column)
        
        print(f"Available stocks with at least {min_data_points} data points: {len(available_stocks)}")
        return available_stocks
    
    def select_random_stocks(self, n_stocks: int, min_data_points: int = 250, 
                           seed: int = None) -> List[str]:
        """
        Randomly select n stocks from available stocks
        """
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)
        
        available_stocks = self.get_available_stocks(min_data_points)
        
        if n_stocks > len(available_stocks):
            print(f"Warning: Only {len(available_stocks)} stocks available, but {n_stocks} requested")
            n_stocks = len(available_stocks)
        
        selected_stocks = random.sample(available_stocks, n_stocks)
        print(f"Selected {len(selected_stocks)} stocks: {selected_stocks}")
        return selected_stocks
    
    def get_returns_window_reverse_chronological(self, end_date: str, window_size: int = 250, 
                                               selected_stocks: List[str] = None) -> pd.DataFrame:
        """
        Get the 250 trading days PRIOR to end_date in REVERSE chronological order
        (latest dates first)
        """
        if selected_stocks is None:
            selected_stocks = self.returns_data.columns.tolist()
        
        # Convert end_date to datetime
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        
        print(f"Requested end date: {end_date}")
        print(f"Looking for {window_size} trading days BEFORE this date")
        
        # Find the position of end_date in the index
        try:
            end_idx = self.returns_data.index.get_loc(end_date)
            print(f"End date found at position: {end_idx}")
        except KeyError:
            print(f"End date {end_date} not found in dataset.")
            # Find the closest date before end_date
            dates_before = self.returns_data[self.returns_data.index <= end_date]
            if len(dates_before) == 0:
                print("No dates found before the requested end date.")
                return pd.DataFrame()
            end_date = dates_before.index[-1]  # Latest date before requested
            end_idx = self.returns_data.index.get_loc(end_date)
            print(f"Using closest available date: {end_date} at position {end_idx}")
        
        # Calculate start index (250 trading days before end_date)
        start_idx = end_idx - window_size + 1
        
        # Check if we have enough data
        if start_idx < 0:
            available_days = end_idx + 1
            print(f"Warning: Only {available_days} trading days available before {end_date}")
            print(f"Need {window_size} days. Using all available data.")
            start_idx = 0
        
        # Extract the window (start_idx to end_idx inclusive) - this is in chronological order
        window_data = self.returns_data.iloc[start_idx:end_idx + 1][selected_stocks].copy()
        
        # REVERSE the order to get latest dates first
        window_data_reversed = window_data.iloc[::-1]
        
        print(f"Successfully extracted {len(window_data_reversed)} trading days")
        print(f"Date range: {window_data_reversed.index[0]} (latest) to {window_data_reversed.index[-1]} (oldest)")
        print(f"Window covers {len(window_data_reversed)} out of requested {window_size} days")
        
        return window_data_reversed
    
    def create_portfolio_dataset(self, n_stocks: int = 80, window_size: int = 250,
                               evaluation_date: str = None, seed: int = None) -> pd.DataFrame:
        """
        Create a complete portfolio dataset for simulation in REVERSE chronological order
        """
        # Select random stocks
        selected_stocks = self.select_random_stocks(n_stocks, window_size, seed)
        
        # Determine evaluation date (default to latest date)
        if evaluation_date is None:
            evaluation_date = self.returns_data.index[-1]  # Latest date (since sorted oldest first)
            print(f"Using latest available date: {evaluation_date}")
        else:
            evaluation_date = pd.to_datetime(evaluation_date)
        
        # Get returns window in reverse chronological order
        portfolio_data = self.get_returns_window_reverse_chronological(evaluation_date, window_size, selected_stocks)
        
        return portfolio_data
    
    def save_portfolio_dataset(self, portfolio_data: pd.DataFrame, 
                             table_name: str = "portfolio_simulation_data"):
        """
        Save portfolio dataset to SQLite database in REVERSE chronological order
        """
        if portfolio_data.empty:
            print("No data to save!")
            return
        
        conn = sqlite3.connect(self.db_file_path)
        
        # Reset index to make date a column (data is already in reverse chronological order)
        save_data = portfolio_data.reset_index()
        
        save_data.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"Portfolio data saved to table: {table_name}")
        print(f"Shape: {save_data.shape}")
        print(f"Date range in saved data: {save_data['date'].min()} to {save_data['date'].max()}")
        print(f"Order: REVERSE chronological (latest first)")
        
        # Verify we have the right number of days and order
        expected_days = min(250, len(portfolio_data))
        actual_days = len(save_data)
        print(f"Expected days: {expected_days}, Actual days: {actual_days}")
        
        conn.close()

# NEW: Function to create tables for each date in a date range
def create_daily_portfolio_datasets(db_file_path: str, start_date: str, end_date: str, 
                                  n_stocks: int = 80, window_size: int = 250,
                                  seed: int = 42):
    """
    Create portfolio datasets for EACH date between start_date and end_date
    Each table contains the 250 trading days prior to that date
    """
    print("=== CREATING DAILY PORTFOLIO DATASETS ===")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Creating tables for each trading day in this range")
    
    # Initialize portfolio data manager
    portfolio_mgr = PortfolioSimulationData(db_file_path)
    
    # Convert dates to datetime
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Get all dates between start and end (inclusive)
    date_range = portfolio_mgr.returns_data[
        (portfolio_mgr.returns_data.index >= start_date) & 
        (portfolio_mgr.returns_data.index <= end_date)
    ].index
    
    print(f"Found {len(date_range)} trading days between {start_date} and {end_date}")
    
    if len(date_range) == 0:
        print("No trading days found in the specified date range!")
        return []
    
    table_names = []
    
    # Use the same stock selection for all dates (for consistency)
    selected_stocks = portfolio_mgr.select_random_stocks(n_stocks, window_size, seed)
    
    for i, current_date in enumerate(date_range):
        print(f"\n--- Creating portfolio for date {i+1}/{len(date_range)}: {current_date} ---")
        
        # Get returns window for this specific date
        portfolio_data = portfolio_mgr.get_returns_window_reverse_chronological(
            current_date, window_size, selected_stocks
        )
        
        # Create table name with date (format: YYYYMMDD)
        date_str = current_date.strftime("%Y%m%d")
        table_name = f"portfolio_{date_str}"
        
        # Save to database
        portfolio_mgr.save_portfolio_dataset(portfolio_data, table_name)
        table_names.append(table_name)
    
    print(f"\n✅ Successfully created {len(table_names)} portfolio datasets")
    print(f"Date range covered: {date_range[0]} to {date_range[-1]}")
    
    return table_names

# NEW: Function to create rolling window datasets with different stock selections
def create_rolling_portfolio_datasets(db_file_path: str, start_date: str, end_date: str, 
                                    n_stocks: int = 80, window_size: int = 250,
                                    same_stocks: bool = False):
    """
    Create portfolio datasets for each date with option for same or different stock selections
    """
    print("=== CREATING ROLLING PORTFOLIO DATASETS ===")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Same stocks for all dates: {same_stocks}")
    
    # Initialize portfolio data manager
    portfolio_mgr = PortfolioSimulationData(db_file_path)
    
    # Convert dates to datetime
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Get all dates between start and end (inclusive)
    date_range = portfolio_mgr.returns_data[
        (portfolio_mgr.returns_data.index >= start_date) & 
        (portfolio_mgr.returns_data.index <= end_date)
    ].index
    
    print(f"Found {len(date_range)} trading days between {start_date} and {end_date}")
    
    if len(date_range) == 0:
        print("No trading days found in the specified date range!")
        return []
    
    table_names = []
    
    # Select stocks once if same_stocks is True
    if same_stocks:
        selected_stocks = portfolio_mgr.select_random_stocks(n_stocks, window_size, seed=42)
        print(f"Using same stocks for all dates: {selected_stocks}")
    
    for i, current_date in enumerate(date_range):
        print(f"\n--- Creating portfolio for date {i+1}/{len(date_range)}: {current_date} ---")
        
        # Select different stocks for each date if same_stocks is False
        if not same_stocks:
            selected_stocks = portfolio_mgr.select_random_stocks(n_stocks, window_size, seed=i)
        
        # Get returns window for this specific date
        portfolio_data = portfolio_mgr.get_returns_window_reverse_chronological(
            current_date, window_size, selected_stocks
        )
        
        # Create table name with date (format: YYYYMMDD)
        date_str = current_date.strftime("%Y%m%d")
        table_name = f"portfolio_{date_str}"
        
        # Save to database
        portfolio_mgr.save_portfolio_dataset(portfolio_data, table_name)
        table_names.append(table_name)
    
    print(f"\n✅ Successfully created {len(table_names)} rolling portfolio datasets")
    
    return table_names

# Verification function
def verify_daily_portfolios(db_file_path: str, start_date: str, end_date: str):
    """Verify all daily portfolio tables were created correctly"""
    conn = sqlite3.connect(db_file_path)
    
    # Convert dates to datetime
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    # Get all portfolio tables in the date range
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name LIKE 'portfolio_%'
    """)
    
    all_portfolio_tables = [table[0] for table in cursor.fetchall()]
    
    # Filter tables within the date range
    relevant_tables = []
    for table_name in all_portfolio_tables:
        try:
            # Extract date from table name (portfolio_YYYYMMDD)
            date_str = table_name.split('_')[1]
            table_date = pd.to_datetime(date_str)
            if start_date <= table_date <= end_date:
                relevant_tables.append(table_name)
        except:
            continue
    
    print(f"\n=== VERIFYING DAILY PORTFOLIOS ===")
    print(f"Found {len(relevant_tables)} portfolio tables between {start_date} and {end_date}")
    
    for table_name in sorted(relevant_tables):
        try:
            portfolio_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            print(f"✅ {table_name}: {len(portfolio_data)} rows, dates: {portfolio_data['date'].min()} to {portfolio_data['date'].max()}")
        except Exception as e:
            print(f"❌ {table_name}: Error - {e}")
    
    conn.close()

# Function to list all portfolio tables in a date range
def list_portfolio_tables_in_range(db_file_path: str, start_date: str, end_date: str):
    """List all portfolio tables within a date range"""
    conn = sqlite3.connect(db_file_path)
    
    # Convert dates to datetime
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name LIKE 'portfolio_%'
    """)
    
    tables_in_range = []
    for table_name in [table[0] for table in cursor.fetchall()]:
        try:
            # Extract date from table name (portfolio_YYYYMMDD)
            date_str = table_name.split('_')[1]
            table_date = pd.to_datetime(date_str)
            if start_date <= table_date <= end_date:
                tables_in_range.append(table_name)
        except:
            continue
    
    print(f"\n=== PORTFOLIO TABLES BETWEEN {start_date} AND {end_date} ===")
    for table in sorted(tables_in_range):
        print(f"  - {table}")
    
    conn.close()
    return tables_in_range

# Usage examples
if __name__ == "__main__":
    db_file = "companies_wide.db"
    
    # Example 1: Create tables for each date in a range (same stocks)
    #print("=== EXAMPLE 1: DAILY PORTFOLIOS (SAME STOCKS) ===")
    #table_names = create_daily_portfolio_datasets(
    #    db_file_path=db_file,
    #    start_date="2025-08-28",
    #    end_date="2025-09-02",
    #    n_stocks=3,
    #    window_size=250,
    #    seed=42
    #)
    
    # Verify the results
    verify_daily_portfolios(db_file, "2025-08-28", "2025-09-02")
    
    # Example 2: Create tables for each date with different stock selections
    print("\n=== EXAMPLE 2: DAILY PORTFOLIOS (DIFFERENT STOCKS) ===")
    table_names2 = create_rolling_portfolio_datasets(
        db_file_path=db_file,
        start_date="2025-08-28",
        end_date="2025-09-02",
        n_stocks=3,
        window_size=250,
        same_stocks=False
    )
    
    # List all tables in the date range
    list_portfolio_tables_in_range(db_file, "2025-08-28", "2025-09-02")