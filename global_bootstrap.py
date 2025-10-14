#!/usr/bin/env python3
"""
global_bootstrap.py - Fixed version with proper encoding handling
Processes SAM.gov data by sub-region with robust character encoding support
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


class GlobalBootstrap:
    """Bootstrap SAM.gov data by processing each sub-region separately"""
    
    def __init__(self):
        self.config = GlobalConfig()
        self.country_manager = GlobalCountryManager()
        self.db_manager = GlobalDatabaseManager(self.config, self.country_manager)
        
        # Progress tracking
        self.progress_file = self.config.data_dir / "bootstrap_progress.json"
        self.completed_segments = self._load_progress()
        
        # Statistics
        self.stats = {
            'by_region': {},
            'by_subregion': {},
            'total_processed': 0,
            'total_found': 0,
            'total_inserted': 0
        }
        
        # Memory optimization
        self.chunk_size = 5000  # Process in smaller chunks
        
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
            "stats": self.stats
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.completed_segments, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")
    
    def detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet"""
        try:
            # Read a sample of the file
            with open(file_path, 'rb') as f:
                raw_data = f.read(100000)  # Read first 100KB
            
            # Detect encoding
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'latin-1')
            confidence = result.get('confidence', 0)
            
            logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
            
            # Use fallback if confidence is low
            if confidence < 0.7:
                encoding = 'latin-1'  # Latin-1 can decode any byte sequence
                logger.info(f"Low confidence, using fallback encoding: {encoding}")
            
            return encoding
        except Exception as e:
            logger.warning(f"Error detecting encoding: {e}, using latin-1")
            return 'latin-1'
    
    def read_csv_with_encoding(self, file_path: Path, chunksize: int = None):
        """Read CSV with automatic encoding detection"""
        # Try encodings in order of likelihood
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
        
        for encoding in encodings:
            try:
                logger.info(f"  Trying to read CSV with {encoding} encoding...")
                
                if chunksize:
                    return pd.read_csv(
                        file_path,
                        chunksize=chunksize,
                        encoding=encoding,
                        dtype=str,
                        on_bad_lines='skip',
                        low_memory=False,
                        engine='python'
                    )
                else:
                    return pd.read_csv(
                        file_path,
                        encoding=encoding,
                        dtype=str,
                        on_bad_lines='skip',
                        low_memory=False,
                        engine='python'
                    )
            except UnicodeDecodeError as e:
                logger.debug(f"  {encoding} failed: {e}")
                continue
            except Exception as e:
                logger.debug(f"  {encoding} failed with other error: {e}")
                continue
        
        # If all encodings fail, try with error handling
        logger.warning("  All standard encodings failed, using latin-1 with error='replace'")
        
        if chunksize:
            return pd.read_csv(
                file_path,
                chunksize=chunksize,
                encoding='latin-1',
                encoding_errors='replace',
                dtype=str,
                on_bad_lines='skip',
                low_memory=False,
                engine='python'
            )
        else:
            return pd.read_csv(
                file_path,
                encoding='latin-1',
                encoding_errors='replace',
                dtype=str,
                on_bad_lines='skip',
                low_memory=False,
                engine='python'
            )
    
    def initialize_database(self):
        """Initialize or clear database"""
        logger.info("Initializing database...")
        
        if not self.config.db_path.exists():
            self.config.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_manager.initialize_database()
        logger.info("Database initialized")
    
    def filter_for_subregion(self, df: pd.DataFrame, region: str, subregion: str) -> pd.DataFrame:
        """Filter dataframe for specific sub-region countries"""
        if df.empty or 'PopCountry' not in df.columns:
            return pd.DataFrame()
        
        # Get countries for this sub-region
        target_countries = self.country_manager.get_countries_by_subregion(region, subregion)
        
        # Filter rows
        filtered_rows = []
        for idx, row in df.iterrows():
            pop_country = str(row.get('PopCountry', '')).strip()
            if not pop_country:
                continue
            
            # Check if this country belongs to our sub-region
            iso3 = self.country_manager.identify_country(pop_country)
            if iso3 and iso3 in target_countries:
                filtered_rows.append(idx)
        
        if filtered_rows:
            return df.loc[filtered_rows].copy()
        
        return pd.DataFrame()
    
    def download_file(self, url: str, dest_path: Path) -> bool:
        """Download file with retry logic"""
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.headers.update({'User-Agent': 'Mozilla/5.0'})
        
        try:
            logger.info(f"Downloading from {url}")
            response = session.get(url, stream=True, timeout=300)
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
    
    def process_year_for_subregion(self, year: int, region: str, subregion: str) -> tuple:
        """Process a single year for a specific sub-region"""
        segment_key = f"{region}_{subregion}_FY{year}"
        
        # Check if already processed successfully
        if segment_key in self.completed_segments:
            if self.completed_segments[segment_key].get('status') == 'completed':
                logger.info(f"  Skipping {segment_key} - already completed")
                stats = self.completed_segments[segment_key].get('stats', {})
                return stats.get('total_found', 0), stats.get('total_inserted', 0)
        
        logger.info(f"  Processing FY{year} for {subregion}...")
        
        # Determine URL
        if year == datetime.now().year or year == datetime.now().year + 1:
            url = self.config.current_csv_url
            source_key = f"CURRENT_{region}_{subregion}"
        else:
            url = f"{self.config.archive_base_url}FY{year}_archived_opportunities.csv?privacy=Public"
            source_key = f"FY{year}_{region}_{subregion}"
        
        found = 0
        inserted = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / f"FY{year}.csv"
            
            # Download file
            if not self.download_file(url, csv_path):
                logger.warning(f"    Could not download FY{year}")
                self._save_progress(segment_key, "download_failed")
                return 0, 0
            
            # Process CSV in chunks with encoding detection
            try:
                chunk_num = 0
                
                # Read CSV with automatic encoding detection
                for chunk in self.read_csv_with_encoding(csv_path, chunksize=self.chunk_size):
                    chunk_num += 1
                    
                    # Filter for this sub-region
                    subregion_data = self.filter_for_subregion(chunk, region, subregion)
                    
                    if not subregion_data.empty:
                        found += len(subregion_data)
                        
                        # Insert into database
                        ins, upd, skip = self.db_manager.insert_or_update_batch(
                            subregion_data, 
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
                
                if region not in self.stats['by_region']:
                    self.stats['by_region'][region] = 0
                self.stats['by_region'][region] += inserted
                
                subregion_key = f"{region}_{subregion}"
                if subregion_key not in self.stats['by_subregion']:
                    self.stats['by_subregion'][subregion_key] = 0
                self.stats['by_subregion'][subregion_key] += inserted
                
                # Save progress
                self._save_progress(segment_key, "completed")
                
            except Exception as e:
                logger.error(f"Error processing {segment_key}: {e}")
                self._save_progress(segment_key, f"error: {str(e)}")
        
        # Clean up memory
        gc.collect()
        
        return found, inserted
    
    def process_subregion(self, region: str, subregion: str, 
                         start_year: int, end_year: int) -> dict:
        """Process all years for a specific sub-region"""
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {region} - {subregion}")
        logger.info(f"Years: FY{start_year} to FY{end_year}")
        logger.info(f"{'='*60}")
        
        total_found = 0
        total_inserted = 0
        
        # Process each year
        for year in range(start_year, end_year + 1):
            found, inserted = self.process_year_for_subregion(year, region, subregion)
            total_found += found
            total_inserted += inserted
            
            # Small delay to avoid overwhelming the server
            time.sleep(1)
        
        logger.info(f"✅ {subregion} complete: {total_found} found, {total_inserted} inserted")
        
        return {
            'found': total_found,
            'inserted': total_inserted
        }
    
    def run_by_subregion(self, target_region: str = None, target_subregion: str = None,
                        start_year: int = None, end_year: int = None,
                        year_increment: int = 5):
        """
        Main bootstrap process - run by sub-region with year increments
        
        Args:
            target_region: Specific region to process (e.g., 'AFRICA')
            target_subregion: Specific sub-region to process  
            start_year: First year to process
            end_year: Last year to process
            year_increment: Process years in chunks of this size
        """
        start_time = datetime.now()
        
        # Determine years
        if end_year is None:
            today = datetime.today()
            end_year = today.year if today.month < 10 else today.year + 1
        
        if start_year is None:
            start_year = end_year - 4  # Default to last 5 years
        
        logger.info("="*60)
        logger.info("Global SAM.gov Bootstrap by Sub-Region")
        logger.info(f"Years: FY{start_year} to FY{end_year}")
        logger.info(f"Year Increment: {year_increment}")
        logger.info("="*60)
        
        # Initialize database if needed
        if not self.config.db_path.exists():
            self.initialize_database()
        
        # Determine what to process
        regions_to_process = {}
        
        if target_region and target_subregion:
            # Process specific sub-region
            regions_to_process = {target_region: [target_subregion]}
        elif target_region:
            # Process all sub-regions in a region
            regions_to_process = {
                target_region: list(self.country_manager.GEOGRAPHIC_REGIONS[target_region].keys())
            }
        else:
            # Process everything
            for region in self.country_manager.GEOGRAPHIC_REGIONS:
                regions_to_process[region] = list(
                    self.country_manager.GEOGRAPHIC_REGIONS[region].keys()
                )
        
        # Process in year increments
        current_start = start_year
        while current_start <= end_year:
            current_end = min(current_start + year_increment - 1, end_year)
            
            logger.info(f"\n{'#'*60}")
            logger.info(f"Processing Years: FY{current_start} to FY{current_end}")
            logger.info(f"{'#'*60}")
            
            # Process each region and sub-region
            for region, subregions in regions_to_process.items():
                for subregion in subregions:
                    result = self.process_subregion(
                        region, subregion, 
                        current_start, current_end
                    )
                    
                    # Give the system a break between sub-regions
                    time.sleep(2)
            
            current_start = current_end + 1
        
        # Final statistics
        elapsed = datetime.now() - start_time
        
        logger.info("\n" + "="*60)
        logger.info("BOOTSTRAP COMPLETE!")
        logger.info("="*60)
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Total opportunities found: {self.stats['total_found']:,}")
        logger.info(f"Total inserted: {self.stats['total_inserted']:,}")
        
        if self.stats['by_region']:
            logger.info("\n📊 Inserted by region:")
            for region, count in self.stats['by_region'].items():
                logger.info(f"  {region}: {count:,}")
        
        logger.info("\n✅ Bootstrap completed successfully!")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Global SAM.gov Bootstrap by Sub-Region"
    )
    parser.add_argument(
        "--region",
        choices=['AFRICA', 'AMERICAS', 'ASIA', 'MIDDLE_EAST', 'EUROPE'],
        help="Specific region to process"
    )
    parser.add_argument(
        "--subregion",
        type=str,
        help="Specific sub-region to process (requires --region)"
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="Start fiscal year"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help="End fiscal year"
    )
    parser.add_argument(
        "--year-increment",
        type=int,
        default=5,
        help="Process years in increments (default: 5)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear database before starting"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode - last 2 years only"
    )
    
    args = parser.parse_args()
    
    # Quick mode overrides
    if args.quick:
        current_year = datetime.now().year
        args.start_year = current_year - 1
        args.end_year = current_year + 1
        args.year_increment = 3
    
    # Create bootstrap instance
    bootstrap = GlobalBootstrap()
    
    # Clear database if requested
    if args.clear:
        bootstrap.initialize_database()
        # Clear progress
        if bootstrap.progress_file.exists():
            bootstrap.progress_file.unlink()
        bootstrap.completed_segments = {}
    
    # Run bootstrap
    bootstrap.run_by_subregion(
        target_region=args.region,
        target_subregion=args.subregion,
        start_year=args.start_year,
        end_year=args.end_year,
        year_increment=args.year_increment
    )


if __name__ == "__main__":
    main()