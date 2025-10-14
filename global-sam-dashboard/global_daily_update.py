#!/usr/bin/env python3
"""
global_daily_update.py - Daily updater for Spirit of America Global Dashboard
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
        
    def run(self):
        logger.info("Starting Global SAM.gov Daily Update")
        
        # Download current CSV
        import requests
        response = requests.get(self.config.current_csv_url, stream=True)
        response.raise_for_status()
        
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk:
                    tmp.write(chunk)
            tmp_path = Path(tmp.name)
        
        # Process recent data
        cutoff = (datetime.now() - timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
        total_inserted = 0
        
        for chunk in pd.read_csv(tmp_path, chunksize=10000, dtype=str, on_bad_lines='skip'):
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
                    inserted, _, _ = self.db_manager.insert_or_update_batch(
                        global_data, source="DAILY_UPDATE"
                    )
                    total_inserted += inserted
        
        # Clean up
        tmp_path.unlink()
        
        logger.info(f"Update complete: {total_inserted} new records added")
        return True

if __name__ == "__main__":
    updater = GlobalDailyUpdater()
    updater.run()