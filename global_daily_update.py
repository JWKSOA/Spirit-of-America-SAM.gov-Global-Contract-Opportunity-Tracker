#!/usr/bin/env python3
"""
global_daily_update.py - Daily updater with proper encoding handling
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_sam_utils import GlobalCountryManager, GlobalConfig, GlobalDatabaseManager

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
    def __init__(self, lookback_days: int = 14):
        self.config = GlobalConfig()
        self.country_manager = GlobalCountryManager()
        self.db_manager = GlobalDatabaseManager(self.config, self.country_manager)
        self.lookback_days = lookback_days
    
    def read_csv_with_encoding(self, file_path: Path, chunksize: int = 10000):
        """Read CSV with automatic encoding detection"""
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                logger.info(f"Trying to read CSV with {encoding} encoding...")
                
                # Use default C engine with low_memory for utf-8
                if encoding == 'utf-8':
                    return pd.read_csv(
                        file_path,
                        chunksize=chunksize,
                        encoding=encoding,
                        dtype=str,
                        on_bad_lines='skip',
                        low_memory=False
                    )
                else:
                    # Use python engine for other encodings, WITHOUT low_memory
                    return pd.read_csv(
                        file_path,
                        chunksize=chunksize,
                        encoding=encoding,
                        dtype=str,
                        on_bad_lines='skip',
                        engine='python'
                    )
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.debug(f"{encoding} failed: {e}")
                continue
        
        # Fallback to latin-1 with error handling
        logger.warning("Using latin-1 with error='replace'")
        return pd.read_csv(
            file_path,
            chunksize=chunksize,
            encoding='latin-1',
            encoding_errors='replace',
            dtype=str,
            on_bad_lines='skip',
            engine='python'
        )
        
    def run(self):
        logger.info("Starting Global SAM.gov Daily Update")
        
        # Download current CSV
        import requests
        
        logger.info("Downloading current opportunities CSV...")
        response = requests.get(self.config.current_csv_url, stream=True)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    tmp.write(chunk)
            tmp_path = Path(tmp.name)
        
        logger.info(f"Downloaded to {tmp_path}")
        
        # Process recent data
        cutoff = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
        total_inserted = 0
        total_updated = 0
        chunk_num = 0
        
        logger.info(f"Processing opportunities posted after {cutoff}")
        
        try:
            for chunk in self.read_csv_with_encoding(tmp_path):
                chunk_num += 1
                
                # Filter for recent dates
                if 'PostedDate' in chunk.columns:
                    chunk['PostedDate_check'] = pd.to_datetime(chunk['PostedDate'], errors='coerce')
                    chunk = chunk[chunk['PostedDate_check'] >= cutoff]
                
                if not chunk.empty:
                    # Filter for valid countries
                    valid_rows = []
                    for idx, row in chunk.iterrows():
                        if self.country_manager.identify_country(row.get('PopCountry', '')):
                            valid_rows.append(idx)
                    
                    if valid_rows:
                        global_data = chunk.loc[valid_rows]
                        inserted, updated, _ = self.db_manager.insert_or_update_batch(
                            global_data, source="DAILY_UPDATE"
                        )
                        total_inserted += inserted
                        total_updated += updated
                        
                        if chunk_num % 10 == 0:
                            logger.info(f"  Processed chunk {chunk_num}: {total_inserted} new, {total_updated} updated")
        
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            return False
        
        finally:
            # Clean up
            tmp_path.unlink()
        
        logger.info(f"Update complete: {total_inserted} new records, {total_updated} updated")
        return True

if __name__ == "__main__":
    updater = GlobalDailyUpdater()
    success = updater.run()
    sys.exit(0 if success else 1)