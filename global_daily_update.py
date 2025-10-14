#!/usr/bin/env python3
"""
global_daily_update.py - Daily updater based on working Africa Dashboard approach
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_sam_utils import GlobalCountryManager, GlobalConfig, GlobalDatabaseManager, CSVReader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'global_update_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GlobalDailyUpdater:
    """Daily updater for global SAM.gov data"""
    
    def __init__(self, lookback_days: int = 14):
        self.config = GlobalConfig()
        self.country_manager = GlobalCountryManager()
        self.db_manager = GlobalDatabaseManager(self.config, self.country_manager)
        self.csv_reader = CSVReader(chunk_size=5000)
        self.lookback_days = lookback_days
        
        # Statistics
        self.stats = {
            'total_processed': 0,
            'total_found': 0,
            'total_inserted': 0,
            'by_region': {}
        }
    
    def run(self) -> bool:
        """Run the daily update process"""
        start_time = datetime.now()
        
        logger.info("="*60)
        logger.info("Global SAM.gov Daily Update")
        logger.info(f"Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Database: {self.config.db_path}")
        logger.info(f"Lookback days: {self.lookback_days}")
        logger.info("="*60)
        
        # Check if database exists
        if not self.config.db_path.exists():
            logger.error("Database not found! Run global_bootstrap.py first.")
            return False
        
        # Get initial statistics
        with self.db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM opportunities")
            initial_count = cur.fetchone()[0]
            logger.info(f"Initial database records: {initial_count:,}")
        
        # Download current CSV
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            logger.info("Downloading current opportunities CSV...")
            try:
                response = requests.get(self.config.current_csv_url, stream=True, timeout=300)
                response.raise_for_status()
                
                with open(csv_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
                
                file_size_mb = csv_path.stat().st_size / (1024 * 1024)
                logger.info(f"Downloaded {file_size_mb:.1f} MB file")
                
            except Exception as e:
                logger.error(f"Failed to download current CSV: {e}")
                
                # Try S3 fallback
                s3_url = self.config.current_csv_url.replace(
                    "https://sam.gov/api/prod/fileextractservices/v1/api/download/",
                    "https://falextracts.s3.amazonaws.com/"
                ).replace("?privacy=Public", "")
                
                logger.info(f"Trying S3 fallback: {s3_url}")
                try:
                    response = requests.get(s3_url, stream=True, timeout=300)
                    response.raise_for_status()
                    
                    with open(csv_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=1024*1024):
                            if chunk:
                                f.write(chunk)
                except Exception as e2:
                    logger.error(f"S3 fallback also failed: {e2}")
                    return False
            
            # Process CSV
            cutoff_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
            logger.info(f"Processing records posted after {cutoff_date}")
            
            chunk_num = 0
            total_inserted = 0
            
            try:
                for chunk in self.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    self.stats['total_processed'] += len(chunk)
                    
                    # Filter for recent dates
                    if 'PostedDate' in chunk.columns:
                        # Normalize dates for comparison
                        chunk['PostedDate_check'] = pd.to_datetime(
                            chunk['PostedDate'].str.split(' ').str[0],
                            errors='coerce'
                        )
                        
                        # Filter for recent records
                        recent_mask = chunk['PostedDate_check'] >= cutoff_date
                        chunk = chunk[recent_mask]
                        
                        if chunk.empty:
                            continue
                    
                    # Filter for valid countries
                    if 'PopCountry' not in chunk.columns:
                        continue
                    
                    valid_rows = []
                    for idx, row in chunk.iterrows():
                        pop_country = str(row.get('PopCountry', '')).strip()
                        if pop_country and self.country_manager.identify_country(pop_country):
                            valid_rows.append(idx)
                    
                    if valid_rows:
                        global_data = chunk.loc[valid_rows].copy()
                        self.stats['total_found'] += len(global_data)
                        
                        # Insert into database
                        inserted, updated, skipped = self.db_manager.insert_or_update_batch(
                            global_data,
                            source="DAILY_UPDATE"
                        )
                        
                        total_inserted += inserted
                        self.stats['total_inserted'] = total_inserted
                        
                        if chunk_num % 10 == 0:
                            logger.info(f"  Chunk {chunk_num}: Processed {self.stats['total_processed']:,}, "
                                      f"found {self.stats['total_found']}, inserted {total_inserted}")
                
            except Exception as e:
                logger.error(f"Error processing CSV: {e}", exc_info=True)
                return False
        
        # Get final statistics
        with self.db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM opportunities")
            final_count = cur.fetchone()[0]
            
            # Get recent by region
            cur.execute("""
                SELECT Geographic_Region, COUNT(*) 
                FROM opportunities 
                WHERE PostedDate_normalized >= ?
                  AND Geographic_Region IS NOT NULL
                GROUP BY Geographic_Region
            """, (cutoff_date,))
            
            recent_by_region = dict(cur.fetchall())
        
        # Generate summary
        elapsed = datetime.now() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("DAILY UPDATE COMPLETE")
        logger.info("="*60)
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Records processed: {self.stats['total_processed']:,}")
        logger.info(f"Valid opportunities found: {self.stats['total_found']:,}")
        logger.info(f"New records inserted: {total_inserted:,}")
        logger.info(f"Database records: {initial_count:,} -> {final_count:,}")
        
        if recent_by_region:
            logger.info("\nRecent records by region:")
            for region, count in recent_by_region.items():
                logger.info(f"  {region}: {count:,}")
        
        logger.info("\n✅ Update completed successfully!")
        return True


def main():
    """Main entry point for daily updates"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Daily update for global SAM.gov data"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Number of days to look back for updates (default: 14)"
    )
    
    args = parser.parse_args()
    
    # Create updater instance
    updater = GlobalDailyUpdater(lookback_days=args.lookback_days)
    
    # Run update
    success = updater.run()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()