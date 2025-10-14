#!/usr/bin/env python3
"""
global_bootstrap.py - Fixed bootstrap based on working Africa Dashboard approach
Processes SAM.gov data for all global regions with proper encoding handling
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import time
import gc
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_sam_utils import GlobalCountryManager, GlobalConfig, GlobalDatabaseManager, CSVReader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('global_bootstrap.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GlobalBootstrap:
    """Bootstrap SAM.gov data for all global regions"""
    
    def __init__(self):
        self.config = GlobalConfig()
        self.country_manager = GlobalCountryManager()
        self.db_manager = GlobalDatabaseManager(self.config, self.country_manager)
        self.csv_reader = CSVReader(chunk_size=5000)
        
        # Progress tracking
        self.progress_file = self.config.data_dir / "bootstrap_progress.json"
        self.completed_segments = self._load_progress()
        
        # HTTP client setup
        self.session = self._create_session()
        
        # Statistics
        self.stats = {
            'by_region': {},
            'by_subregion': {},
            'total_processed': 0,
            'total_found': 0,
            'total_inserted': 0
        }
        
    def _create_session(self) -> requests.Session:
        """Create session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        return session
    
    def _load_progress(self) -> dict:
        """Load progress from file"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_progress(self, segment_key: str, status: str = "completed"):
        """Save progress after completing a segment"""
        self.completed_segments[segment_key] = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "stats": {
                'total_found': self.stats['total_found'],
                'total_inserted': self.stats['total_inserted']
            }
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.completed_segments, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def download_file(self, url: str, dest_path: Path) -> bool:
        """Download file with retry logic"""
        try:
            logger.info(f"Downloading from {url}")
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0 and downloaded % (50 * 1024 * 1024) == 0:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"  Download progress: {progress:.1f}%")
            
            logger.info(f"  Download complete: {dest_path.name}")
            return True
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            
            # Try S3 fallback
            if "sam.gov" in url:
                s3_url = url.replace(
                    "https://sam.gov/api/prod/fileextractservices/v1/api/download/",
                    "https://falextracts.s3.amazonaws.com/"
                ).replace("?privacy=Public", "")
                
                logger.info(f"Trying S3 fallback: {s3_url}")
                return self.download_file(s3_url, dest_path)
            
            return False
    
    def process_year_for_region(self, year: int, target_region: str = None) -> tuple:
        """Process a single year, optionally filtered by region"""
        
        # Create segment key
        if target_region:
            segment_key = f"{target_region}_FY{year}"
        else:
            segment_key = f"GLOBAL_FY{year}"
        
        # Check if already processed
        if segment_key in self.completed_segments:
            if self.completed_segments[segment_key].get('status') == 'completed':
                logger.info(f"  Skipping {segment_key} - already completed")
                stats = self.completed_segments[segment_key].get('stats', {})
                return stats.get('total_found', 0), stats.get('total_inserted', 0)
        
        logger.info(f"  Processing FY{year}...")
        
        # Determine URL
        if year == datetime.now().year or year == datetime.now().year + 1:
            url = self.config.current_csv_url
            source_key = f"CURRENT_{target_region or 'GLOBAL'}"
        else:
            url = f"{self.config.archive_base_url}FY{year}_archived_opportunities.csv?privacy=Public"
            source_key = f"FY{year}_{target_region or 'GLOBAL'}"
        
        found = 0
        inserted = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / f"FY{year}.csv"
            
            # Download file
            if not self.download_file(url, csv_path):
                logger.warning(f"    Could not download FY{year}")
                self._save_progress(segment_key, "download_failed")
                return 0, 0
            
            # Process CSV in chunks using fixed CSV reader
            try:
                chunk_num = 0
                
                for chunk in self.csv_reader.read_csv_chunks(csv_path):
                    chunk_num += 1
                    self.stats['total_processed'] += len(chunk)
                    
                    # Filter for valid countries
                    if 'PopCountry' not in chunk.columns:
                        continue
                    
                    valid_rows = []
                    for idx, row in chunk.iterrows():
                        pop_country = str(row.get('PopCountry', '')).strip()
                        if not pop_country:
                            continue
                        
                        # Check if this is a valid country
                        iso3 = self.country_manager.identify_country(pop_country)
                        if not iso3:
                            continue
                        
                        # If filtering by region, check if country belongs to that region
                        if target_region:
                            region_info = self.country_manager.get_region(iso3)
                            if not region_info or region_info[0] != target_region:
                                continue
                        
                        valid_rows.append(idx)
                    
                    if valid_rows:
                        filtered_data = chunk.loc[valid_rows].copy()
                        found += len(filtered_data)
                        
                        # Insert into database
                        ins, upd, skip = self.db_manager.insert_or_update_batch(
                            filtered_data,
                            source=source_key
                        )
                        inserted += ins
                        
                        if chunk_num % 20 == 0:
                            logger.info(f"    Chunk {chunk_num}: {found} found, {inserted} inserted")
                    
                    # Clean memory periodically
                    if chunk_num % 50 == 0:
                        gc.collect()
                
                logger.info(f"    FY{year} complete: {found} found, {inserted} inserted")
                
                # Update statistics
                self.stats['total_found'] += found
                self.stats['total_inserted'] += inserted
                
                # Save progress
                self._save_progress(segment_key, "completed")
                
            except Exception as e:
                logger.error(f"Error processing {segment_key}: {e}")
                self._save_progress(segment_key, f"error: {str(e)}")
        
        # Clean up memory
        gc.collect()
        
        return found, inserted
    
    def initialize_database(self):
        """Initialize or clear database"""
        logger.info("Initializing database...")
        
        if not self.config.db_path.exists():
            self.config.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_manager.initialize_database()
        logger.info("Database initialized")
    
    def run_bootstrap(self, start_year: int = None, end_year: int = None,
                     target_region: str = None, clear: bool = False):
        """
        Run bootstrap process
        
        Args:
            start_year: First year to process
            end_year: Last year to process  
            target_region: Specific region to process (e.g., 'AFRICA', 'ASIA')
            clear: Whether to clear existing database
        """
        start_time = datetime.now()
        
        # Determine years
        if end_year is None:
            today = datetime.today()
            end_year = today.year if today.month < 10 else today.year + 1
        
        if start_year is None:
            start_year = end_year - 2  # Default to last 3 years
        
        logger.info("="*60)
        logger.info("Global SAM.gov Bootstrap")
        logger.info(f"Years: FY{start_year} to FY{end_year}")
        if target_region:
            logger.info(f"Target Region: {target_region}")
        else:
            logger.info("Processing all regions")
        logger.info("="*60)
        
        # Initialize database if needed
        if clear or not self.config.db_path.exists():
            self.initialize_database()
            # Clear progress
            if self.progress_file.exists():
                self.progress_file.unlink()
            self.completed_segments = {}
        
        # Process each year
        for year in range(start_year, end_year + 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing FY{year}")
            logger.info(f"{'='*60}")
            
            found, inserted = self.process_year_for_region(year, target_region)
            
            # Give the system a break between years
            time.sleep(2)
        
        # Final statistics
        elapsed = datetime.now() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("BOOTSTRAP COMPLETE!")
        logger.info("="*60)
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Total records processed: {self.stats['total_processed']:,}")
        logger.info(f"Total opportunities found: {self.stats['total_found']:,}")
        logger.info(f"Total inserted: {self.stats['total_inserted']:,}")
        
        # Show database statistics
        try:
            with self.db_manager.get_connection() as conn:
                cur = conn.cursor()
                
                # Total records
                cur.execute("SELECT COUNT(*) FROM opportunities")
                total_db = cur.fetchone()[0]
                logger.info(f"\nDatabase now contains: {total_db:,} total records")
                
                # By region
                cur.execute("""
                    SELECT Geographic_Region, COUNT(*) 
                    FROM opportunities 
                    WHERE Geographic_Region IS NOT NULL
                    GROUP BY Geographic_Region
                    ORDER BY COUNT(*) DESC
                """)
                
                logger.info("\n📊 Records by region:")
                for region, count in cur.fetchall():
                    logger.info(f"  {region}: {count:,}")
                
        except Exception as e:
            logger.error(f"Could not get final statistics: {e}")
        
        logger.info("\n✅ Bootstrap completed successfully!")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Global SAM.gov Bootstrap"
    )
    parser.add_argument(
        "--region",
        choices=['AFRICA', 'AMERICAS', 'ASIA', 'MIDDLE_EAST', 'EUROPE'],
        help="Specific region to process"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Start fiscal year (default: 2 years ago)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="End fiscal year (default: current)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear database before starting"
    )
    
    args = parser.parse_args()
    
    # Create bootstrap instance
    bootstrap = GlobalBootstrap()
    
    # Run bootstrap
    bootstrap.run_bootstrap(
        start_year=args.start_year,
        end_year=args.end_year,
        target_region=args.region,
        clear=args.clear
    )


if __name__ == "__main__":
    main()