#!/usr/bin/env python3
"""
global_bootstrap.py - Complete historical data loader for Spirit of America Global Dashboard
Processes all SAM.gov archive files and current data with global country identification
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import json

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_sam_utils import GlobalCountryManager, GlobalConfig, GlobalDatabaseManager

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

class GlobalHistoricalBootstrap:
    """Complete bootstrap of all SAM.gov historical data for global tracking"""
    
    def __init__(self):
        """Initialize bootstrap system"""
        self.config = GlobalConfig()
        self.country_manager = GlobalCountryManager()
        self.db_manager = GlobalDatabaseManager(self.config, self.country_manager)
        self.progress_file = self.config.data_dir / "global_bootstrap_progress.json"
        self.completed_sources = self._load_progress()
        
        # Statistics tracking
        self.total_inserted = 0
        self.total_skipped = 0
        self.region_stats = {}
        
        # HTTP session for downloads
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
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
    
    def _save_progress(self, source: str, status: str = "completed"):
        """Save progress after completing a source"""
        self.completed_sources[source] = {
            "status": status,
            "timestamp": datetime.now().isoformat(),
            "records": {
                "inserted": self.total_inserted,
                "skipped": self.total_skipped
            }
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.completed_sources, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def clear_database(self):
        """Clear and reinitialize database"""
        logger.warning("Clearing existing database...")
        
        if self.config.db_path.exists():
            self.config.db_path.unlink()
        
        # Initialize with proper schema
        self.db_manager.initialize_database()
        
        # Clear progress tracking
        if self.progress_file.exists():
            self.progress_file.unlink()
        self.completed_sources = {}
        
        logger.info("Database cleared and initialized for global tracking")
    
    def download_file(self, url: str, dest_path: Path) -> bool:
        """Download file with progress indication"""
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
                        
                        if total_size > 0 and downloaded % (10 * 1024 * 1024) == 0:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"Progress: {progress:.1f}%")
            
            logger.info(f"Download complete: {dest_path}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            
            # Try S3 fallback
            if "sam.gov" in url and "s3.amazonaws.com" not in url:
                s3_url = url.replace(
                    "https://sam.gov/api/prod/fileextractservices/v1/api/download/",
                    "https://falextracts.s3.amazonaws.com/"
                ).replace("?privacy=Public", "")
                
                logger.info(f"Trying S3 fallback URL: {s3_url}")
                return self.download_file(s3_url, dest_path)
            
            return False
    
    def process_csv_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of SAM.gov data for global countries"""
        if chunk.empty:
            return chunk
        
        # Ensure PopCountry column exists
        if 'PopCountry' not in chunk.columns:
            logger.warning("No PopCountry column found in chunk")
            return pd.DataFrame()
        
        # Identify valid countries
        valid_countries = []
        for idx, row in chunk.iterrows():
            pop_country = row.get('PopCountry', '')
            iso3 = self.country_manager.identify_country(pop_country)
            if iso3:
                valid_countries.append(idx)
        
        # Filter for valid countries only
        global_data = chunk.loc[valid_countries].copy()
        
        if not global_data.empty:
            logger.info(f"Found {len(global_data)} global opportunities in chunk")
        
        return global_data
    
    def process_archive_year(self, year: int) -> Tuple[int, int]:
        """Process a single fiscal year archive"""
        source_key = f"FY{year}"
        
        # Check if already processed
        if source_key in self.completed_sources:
            status = self.completed_sources[source_key].get('status')
            if status == 'completed':
                logger.info(f"Skipping {source_key} - already completed")
                return 0, 0
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {source_key} Archive")
        logger.info(f"{'='*60}")
        
        # Get archive URL
        archive_url = f"{self.config.archive_base_url}FY{year}_archived_opportunities.csv?privacy=Public"
        
        year_inserted = 0
        year_skipped = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / f"FY{year}.csv"
            
            # Download archive
            if not self.download_file(archive_url, csv_path):
                logger.warning(f"Could not download FY{year} archive - may not exist")
                self._save_progress(source_key, "not_found")
                return 0, 0
            
            # Check file size
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)
            logger.info(f"Processing {file_size_mb:.1f} MB file")
            
            # Process CSV in chunks
            try:
                chunk_num = 0
                for chunk in pd.read_csv(csv_path, chunksize=self.config.chunk_size, 
                                        dtype=str, on_bad_lines='skip', low_memory=False):
                    chunk_num += 1
                    
                    # Filter for valid global countries
                    global_data = self.process_csv_chunk(chunk)
                    
                    if not global_data.empty:
                        # Insert with region identification
                        inserted, updated, skipped = self.db_manager.insert_or_update_batch(
                            global_data, 
                            source=source_key
                        )
                        
                        year_inserted += inserted
                        year_skipped += skipped
                        
                        # Update region statistics
                        for _, row in global_data.iterrows():
                            pop_country = row.get('PopCountry', '')
                            iso3 = self.country_manager.identify_country(pop_country)
                            if iso3:
                                region_info = self.country_manager.get_region(iso3)
                                if region_info:
                                    region, _ = region_info
                                    if region not in self.region_stats:
                                        self.region_stats[region] = 0
                                    self.region_stats[region] += 1
                        
                        # Log progress every 10 chunks
                        if chunk_num % 10 == 0:
                            logger.info(f"  Chunk {chunk_num}: {year_inserted} new, {year_skipped} skipped")
                
                # Update totals
                self.total_inserted += year_inserted
                self.total_skipped += year_skipped
                
                # Save progress
                self._save_progress(source_key, "completed")
                
                logger.info(f"✅ {source_key} complete: {year_inserted} inserted, {year_skipped} skipped")
                
            except Exception as e:
                logger.error(f"Error processing {source_key}: {e}")
                self._save_progress(source_key, "error")
        
        return year_inserted, year_skipped
    
    def process_current_data(self) -> Tuple[int, int]:
        """Process current opportunities CSV"""
        source_key = "CURRENT"
        
        logger.info(f"\n{'='*60}")
        logger.info("Processing Current Opportunities CSV")
        logger.info(f"{'='*60}")
        
        current_url = self.config.current_csv_url
        
        current_inserted = 0
        current_skipped = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "current.csv"
            
            # Download current CSV
            if not self.download_file(current_url, csv_path):
                logger.error("Failed to download current CSV")
                return 0, 0
            
            # Check file size
            file_size_mb = csv_path.stat().st_size / (1024 * 1024)
            logger.info(f"Processing {file_size_mb:.1f} MB current file")
            
            # Process CSV in chunks
            try:
                chunk_num = 0
                for chunk in pd.read_csv(csv_path, chunksize=self.config.chunk_size,
                                        dtype=str, on_bad_lines='skip', low_memory=False):
                    chunk_num += 1
                    
                    # Filter for global countries
                    global_data = self.process_csv_chunk(chunk)
                    
                    if not global_data.empty:
                        inserted, updated, skipped = self.db_manager.insert_or_update_batch(
                            global_data,
                            source=source_key
                        )
                        
                        current_inserted += inserted
                        current_skipped += skipped
                        
                        # Log progress
                        if chunk_num % 10 == 0:
                            logger.info(f"  Chunk {chunk_num}: {current_inserted} new, {current_skipped} skipped")
                
                # Update totals
                self.total_inserted += current_inserted
                self.total_skipped += current_skipped
                
                # Save progress
                self._save_progress(source_key, "completed")
                
                logger.info(f"✅ Current data complete: {current_inserted} inserted, {current_skipped} skipped")
                
            except Exception as e:
                logger.error(f"Error processing current data: {e}")
                self._save_progress(source_key, "error")
        
        return current_inserted, current_skipped
    
    def optimize_database(self):
        """Optimize database after loading all data"""
        logger.info("\n🔧 Optimizing database...")
        
        with self.db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Ensure all dates are normalized
            logger.info("Normalizing any remaining dates...")
            cur.execute("""
                UPDATE opportunities 
                SET PostedDate_normalized = 
                    CASE 
                        WHEN PostedDate LIKE '____-__-__ __-__-__' 
                            THEN substr(PostedDate, 1, 10)
                        WHEN PostedDate LIKE '____-__-__' 
                            THEN PostedDate
                        ELSE PostedDate_normalized
                    END
                WHERE PostedDate_normalized IS NULL 
                  AND PostedDate IS NOT NULL
            """)
            
            normalized = cur.rowcount
            if normalized > 0:
                logger.info(f"  Normalized {normalized} dates")
            
            # Update statistics
            cur.execute("ANALYZE")
            conn.commit()
        
        # Run VACUUM in separate connection
        import sqlite3
        conn = sqlite3.connect(str(self.config.db_path))
        conn.execute("VACUUM")
        conn.close()
        
        logger.info("✅ Database optimized")
    
    def run(self, start_year: int = 2020, end_year: Optional[int] = None, 
            clear_first: bool = False, skip_current: bool = False):
        """
        Run complete bootstrap process
        
        Args:
            start_year: First fiscal year to process (default 2020 for faster initial load)
            end_year: Last fiscal year to process (default current FY)
            clear_first: Whether to clear database first
            skip_current: Whether to skip current data
        """
        start_time = datetime.now()
        
        # Determine end year
        if end_year is None:
            today = datetime.today()
            end_year = today.year if today.month < 10 else today.year + 1
        
        logger.info("="*60)
        logger.info("Spirit of America Global SAM.gov Bootstrap")
        logger.info(f"Processing FY{start_year} through FY{end_year}")
        logger.info(f"Database: {self.config.db_path}")
        logger.info("="*60)
        
        # Clear if requested
        if clear_first:
            self.clear_database()
        elif not self.config.db_path.exists():
            self.db_manager.initialize_database()
        
        # Get initial statistics
        initial_stats = self.db_manager.get_statistics_by_region()
        logger.info(f"Starting with {initial_stats['total_records']:,} records")
        
        # Process all archive years
        years_to_process = list(range(start_year, end_year + 1))
        
        logger.info(f"Processing {len(years_to_process)} archive years...")
        
        for year in years_to_process:
            self.process_archive_year(year)
        
        # Process current data
        if not skip_current:
            self.process_current_data()
        
        # Optimize database
        self.optimize_database()
        
        # Get final statistics
        final_stats = self.db_manager.get_statistics_by_region()
        
        # Generate report
        elapsed = datetime.now() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("GLOBAL BOOTSTRAP COMPLETE!")
        logger.info("="*60)
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Total inserted: {self.total_inserted:,}")
        logger.info(f"Total skipped: {self.total_skipped:,}")
        logger.info(f"Final database records: {final_stats['total_records']:,}")
        
        logger.info("\n📊 Records by region:")
        for region, count in final_stats.get('by_region', {}).items():
            if region:
                logger.info(f"  {region.replace('_', ' ').title()}: {count:,}")
        
        logger.info("\n📈 Recent activity (last 30 days) by region:")
        for region, count in final_stats.get('recent_by_region', {}).items():
            if region:
                logger.info(f"  {region.replace('_', ' ').title()}: {count:,}")
        
        # Clean up progress file if complete
        if self.progress_file.exists():
            self.progress_file.unlink()
        
        logger.info("\n✅ Global bootstrap completed successfully!")
        logger.info("Your Spirit of America dashboard now has complete global data!")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Bootstrap Spirit of America Global SAM.gov data"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Start fiscal year (default: 2020 for faster initial load)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End fiscal year (default: current FY)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing database before loading"
    )
    parser.add_argument(
        "--skip-current",
        action="store_true",
        help="Skip loading current opportunities"
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Load full history from 1998 (takes longer)"
    )
    
    args = parser.parse_args()
    
    # Adjust start year if full history requested
    if args.full_history:
        args.start_year = 1998
    
    # Print startup message
    print("="*60)
    print("Starting Spirit of America Global Bootstrap")
    print(f"Time: {datetime.now()}")
    print(f"Arguments: {args}")
    print("="*60)
    
    # Create bootstrap instance
    bootstrap = GlobalHistoricalBootstrap()
    
    # Run bootstrap
    bootstrap.run(
        start_year=args.start_year,
        end_year=args.end_year,
        clear_first=args.clear,
        skip_current=args.skip_current
    )


if __name__ == "__main__":
    main()