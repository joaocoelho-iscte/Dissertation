import sqlite3
import pandas as pd
import numpy as np
from typing import List, Dict, Set
import random

class PortfolioBootstrap:
    """
    Class to create bootstrap samples from portfolio tables
    """
    
    def __init__(self, db_file_path: str):
        self.db_file_path = db_file_path
    
    def list_all_tables(self) -> List[str]:
        """List all tables in the database"""
        conn = sqlite3.connect(self.db_file_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        all_tables = [table[0] for table in cursor.fetchall()]
        conn.close()
        return all_tables
    
    def detect_existing_bootstrap_tables(self) -> Set[str]:
        """Detect which portfolio tables already have bootstrap versions"""
        all_tables = self.list_all_tables()
        
        # Find all bootstrap tables and extract their original table names
        existing_originals = set()
        for table_name in all_tables:
            if table_name.endswith('_bootstrap'):
                original_name = table_name.replace('_bootstrap', '')
                existing_originals.add(original_name)
        
        print(f"Found {len(existing_originals)} portfolio tables with existing bootstrap samples")
        for table in sorted(existing_originals):
            print(f"  - {table} → {table}_bootstrap")
        
        return existing_originals
    
    def list_portfolio_tables(self, start_date: str = None, end_date: str = None, 
                            exclude_existing: bool = True) -> List[str]:
        """
        List portfolio tables, optionally filtered by date range and excluding existing bootstrap tables
        """
        all_tables = self.list_all_tables()
        
        # Filter for portfolio tables
        portfolio_tables = [table for table in all_tables if table.startswith('portfolio_') 
                          and not table.endswith('_bootstrap')]
        
        # Filter by date range if provided
        if start_date or end_date:
            filtered_tables = []
            start_date = pd.to_datetime(start_date) if start_date else None
            end_date = pd.to_datetime(end_date) if end_date else None
            
            for table_name in portfolio_tables:
                try:
                    # Extract date from table name (portfolio_YYYYMMDD)
                    date_str = table_name.split('_')[1]
                    table_date = pd.to_datetime(date_str)
                    
                    # Check if within date range
                    if start_date and table_date < start_date:
                        continue
                    if end_date and table_date > end_date:
                        continue
                    
                    filtered_tables.append(table_name)
                except:
                    continue
            
            portfolio_tables = filtered_tables
        
        # Exclude tables that already have bootstrap versions
        if exclude_existing:
            existing_bootstrap_tables = self.detect_existing_bootstrap_tables()
            portfolio_tables = [table for table in portfolio_tables if table not in existing_bootstrap_tables]
        
        print(f"Found {len(portfolio_tables)} portfolio tables to process")
        for table in sorted(portfolio_tables):
            print(f"  - {table}")
        
        return sorted(portfolio_tables)
    
    def load_portfolio_data(self, table_name: str) -> pd.DataFrame:
        """
        Load portfolio data from a specific table
        """
        conn = sqlite3.connect(self.db_file_path)
        portfolio_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        portfolio_data['date'] = pd.to_datetime(portfolio_data['date'])
        conn.close()
        
        print(f"Loaded {table_name}: {portfolio_data.shape}")
        return portfolio_data
    
    def create_bootstrap_sample(self, portfolio_data: pd.DataFrame, n_samples: int = 1000, 
                              seed: int = None) -> pd.DataFrame:
        """
        Create bootstrap samples from portfolio data (sampling with replacement)
        """
        if seed is not None:
            np.random.seed(seed)
            random.seed(seed)
        
        n_original = len(portfolio_data)
        
        # Randomly select rows with replacement
        bootstrap_indices = np.random.choice(n_original, size=n_samples, replace=True)
        bootstrap_sample = portfolio_data.iloc[bootstrap_indices].copy()
        
        # Reset index and add bootstrap iteration number
        bootstrap_sample = bootstrap_sample.reset_index(drop=True)
        bootstrap_sample['bootstrap_iteration'] = range(1, n_samples + 1)
        
        print(f"Created bootstrap sample: {bootstrap_sample.shape}")
        print(f"Original data: {n_original} rows → Bootstrap: {n_samples} samples")
        
        return bootstrap_sample
    
    def create_bootstrap_for_table(self, table_name: str, n_samples: int = 1000, 
                                 seed: int = None) -> pd.DataFrame:
        """
        Create bootstrap samples for a specific portfolio table
        """
        print(f"\n--- Creating bootstrap samples for {table_name} ---")
        
        # Load portfolio data
        portfolio_data = self.load_portfolio_data(table_name)
        
        # Create bootstrap samples
        bootstrap_data = self.create_bootstrap_sample(portfolio_data, n_samples, seed)
        
        return bootstrap_data
    
    def save_bootstrap_sample(self, bootstrap_data: pd.DataFrame, 
                            original_table_name: str, 
                            suffix: str = "bootstrap"):
        """
        Save bootstrap sample to database
        """
        table_name = f"{original_table_name}_{suffix}"
        
        conn = sqlite3.connect(self.db_file_path)
        bootstrap_data.to_sql(table_name, conn, if_exists='replace', index=False)
        
        print(f"Bootstrap sample saved to: {table_name}")
        print(f"Shape: {bootstrap_data.shape}")
        
        conn.close()
        
        return table_name
    
    def create_bootstrap_for_all_tables(self, n_samples: int = 1000, 
                                      start_date: str = None, end_date: str = None,
                                      seed: int = None, overwrite: bool = False) -> Dict[str, str]:
        """
        Create bootstrap samples for portfolio tables
        - Only processes tables that don't have existing bootstrap versions (unless overwrite=True)
        """
        # List portfolio tables to process
        if overwrite:
            portfolio_tables = self.list_portfolio_tables(start_date, end_date, exclude_existing=False)
        else:
            portfolio_tables = self.list_portfolio_tables(start_date, end_date, exclude_existing=True)
        
        if not portfolio_tables:
            print("No portfolio tables to process!")
            return {}
        
        bootstrap_tables = {}
        
        for i, table_name in enumerate(portfolio_tables):
            print(f"\n{'='*50}")
            print(f"Processing {i+1}/{len(portfolio_tables)}: {table_name}")
            print(f"{'='*50}")
            
            # Create bootstrap samples
            bootstrap_data = self.create_bootstrap_for_table(table_name, n_samples, seed)
            
            # Save to database
            bootstrap_table_name = self.save_bootstrap_sample(bootstrap_data, table_name)
            bootstrap_tables[table_name] = bootstrap_table_name
        
        print(f"\n✅ Successfully created {len(bootstrap_tables)} bootstrap tables")
        return bootstrap_tables

# Smart bootstrap manager that tracks progress
class SmartBootstrapManager:
    """
    Advanced class that tracks which tables have been processed and can resume interrupted jobs
    """
    
    def __init__(self, db_file_path: str):
        self.db_file_path = db_file_path
        self.bootstrap_gen = PortfolioBootstrap(db_file_path)
    
    def get_processing_status(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Get detailed status of which portfolio tables have bootstrap versions
        """
        all_tables = self.bootstrap_gen.list_all_tables()
        
        # Get portfolio tables
        portfolio_tables = [table for table in all_tables if table.startswith('portfolio_') 
                          and not table.endswith('_bootstrap')]
        
        # Filter by date range
        if start_date or end_date:
            filtered_tables = []
            start_date = pd.to_datetime(start_date) if start_date else None
            end_date = pd.to_datetime(end_date) if end_date else None
            
            for table_name in portfolio_tables:
                try:
                    date_str = table_name.split('_')[1]
                    table_date = pd.to_datetime(date_str)
                    
                    if start_date and table_date < start_date:
                        continue
                    if end_date and table_date > end_date:
                        continue
                    
                    filtered_tables.append(table_name)
                except:
                    continue
            
            portfolio_tables = filtered_tables
        
        # Create status report
        status_data = []
        for table_name in sorted(portfolio_tables):
            bootstrap_table = f"{table_name}_bootstrap"
            has_bootstrap = bootstrap_table in all_tables
            
            # Get row counts if bootstrap exists
            original_rows = bootstrap_rows = 0
            try:
                conn = sqlite3.connect(self.db_file_path)
                original_rows = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {table_name}", conn)['cnt'].iloc[0]
                if has_bootstrap:
                    bootstrap_rows = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {bootstrap_table}", conn)['cnt'].iloc[0]
                conn.close()
            except:
                pass
            
            status_data.append({
                'portfolio_table': table_name,
                'has_bootstrap': has_bootstrap,
                'bootstrap_table': bootstrap_table if has_bootstrap else 'None',
                'original_rows': original_rows,
                'bootstrap_rows': bootstrap_rows,
                'status': 'COMPLETED' if has_bootstrap else 'PENDING'
            })
        
        status_df = pd.DataFrame(status_data)
        return status_df
    
    def print_processing_status(self, start_date: str = None, end_date: str = None):
        """Print a nice status report"""
        status_df = self.get_processing_status(start_date, end_date)
        
        print(f"\n=== BOOTSTRAP PROCESSING STATUS ===")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Total portfolio tables: {len(status_df)}")
        
        completed = len(status_df[status_df['has_bootstrap']])
        pending = len(status_df[~status_df['has_bootstrap']])
        
        print(f"Completed: {completed}")
        print(f"Pending: {pending}")
        
        if pending > 0:
            print(f"\nPending tables:")
            for _, row in status_df[~status_df['has_bootstrap']].iterrows():
                print(f"  - {row['portfolio_table']}")
        
        return status_df
    
    def process_missing_tables(self, n_samples: int = 1000, 
                             start_date: str = None, end_date: str = None,
                             seed: int = None) -> Dict[str, str]:
        """
        Only process tables that don't have bootstrap versions yet
        """
        status_df = self.get_processing_status(start_date, end_date)
        missing_tables = status_df[~status_df['has_bootstrap']]['portfolio_table'].tolist()
        
        if not missing_tables:
            print("All tables already have bootstrap samples!")
            return {}
        
        print(f"Processing {len(missing_tables)} missing tables...")
        return self.bootstrap_gen.create_bootstrap_for_all_tables(
            n_samples=n_samples,
            start_date=start_date,
            end_date=end_date,
            seed=seed,
            overwrite=False  # Only process missing tables
        )

# Main execution functions
def create_bootstrap_samples_smart(db_file_path: str, n_samples: int = 1000,
                                 start_date: str = None, end_date: str = None,
                                 seed: int = 42, overwrite: bool = False):
    """
    Smart function that detects existing bootstrap tables and only processes missing ones
    """
    print("=== SMART BOOTSTRAP SAMPLING ===")
    print(f"Number of samples per table: {n_samples}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Overwrite existing: {overwrite}")
    
    # Initialize smart manager
    manager = SmartBootstrapManager(db_file_path)
    
    # Show current status
    manager.print_processing_status(start_date, end_date)
    
    # Process tables
    if overwrite:
        # Process all tables (overwrite existing)
        bootstrap_gen = PortfolioBootstrap(db_file_path)
        bootstrap_tables = bootstrap_gen.create_bootstrap_for_all_tables(
            n_samples=n_samples,
            start_date=start_date,
            end_date=end_date,
            seed=seed,
            overwrite=True
        )
    else:
        # Only process missing tables
        bootstrap_tables = manager.process_missing_tables(
            n_samples=n_samples,
            start_date=start_date,
            end_date=end_date,
            seed=seed
        )
    
    # Show final status
    print(f"\n=== FINAL STATUS ===")
    manager.print_processing_status(start_date, end_date)
    
    return bootstrap_tables

# Usage examples
if __name__ == "__main__":
    db_file = "companies_wide.db"
    
    # Example 1: Smart processing (only missing tables)
    print("=== EXAMPLE 1: SMART PROCESSING (ONLY MISSING TABLES) ===")
    bootstrap_tables = create_bootstrap_samples_smart(
        db_file_path=db_file,
        n_samples=1000,
        start_date="2025-08-28",
        end_date="2025-09-02",
        seed=42,
        overwrite=False  # Only process tables that don't have bootstrap versions
    )
    
    # Example 2: Force overwrite all tables
    #print("\n=== EXAMPLE 2: FORCE OVERWRITE ALL TABLES ===")
    #bootstrap_tables = create_bootstrap_samples_smart(
    #    db_file_path=db_file,
    #    n_samples=1000,
    #    start_date="2025-08-28", 
    #    end_date="2025-09-02",
    #    seed=42,
    #    overwrite=True  # Recreate bootstrap for all tables
    #)
    
    # Example 3: Just check status without processing
    print("\n=== EXAMPLE 3: STATUS CHECK ONLY ===")
    manager = SmartBootstrapManager(db_file)
    status_df = manager.print_processing_status("2025-08-28", "2025-09-02")