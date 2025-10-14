#!/usr/bin/env python3
"""
global_bootstrap_optimized.py - Optimized bootstrap that processes data by sub-region
Handles massive SAM.gov data by breaking it down into manageable chunks
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict
import json
import time
import gc

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
        logging.FileHandler('global_bootstrap_optimized.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OptimizedGlobalBootstrap:
    """Optimized bootstrap that processes by sub-region to handle massive data"""
    
    def __init__(self):
        """Initialize optimized bootstrap system"""
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
            'total_found': 0
        }
        
        # HTTP session
        self.session = self._create_session()
        
        # Memory optimization
        self.chunk_size = 5000  # Smaller chunks for memory efficiency
    
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
    
    def clear_and_init_database(self):
        """Clear and initialize database"""
        logger.info("Initializing database...")
        
        if self.config.db_path.exists():
            self.config.db_path.unlink()
        
        self.db_manager.initialize_database()
        
        # Clear progress
        if self.progress_file.exists():
            self.progress_file.unlink()
        self.completed_segments = {}
        
        logger.info("Database initialized")
    
    def get_subregion_countries(self, region: str, subregion: str) -> List[str]:
        """Get all country variations for a sub-region"""
        countries = []
        
        # Get the countries for this sub-region
        subregion_data = self.country_manager.GEOGRAPHIC_REGIONS.get(region, {}).get(subregion, {})
        
        for country_name, iso3 in subregion_data.items():
            # Add the main country name
            countries.append(country_name.upper())
            countries.append(iso3)
            
            # Add common variations
            countries.append(country_name.lower())
            countries.append(country_name.replace("'", ""))
            countries.append(country_name.replace("-", " "))
            
            # Add from alternative names
            for alt_name, alt_iso3 in self.country_manager.ALTERNATIVE_NAMES.items():
                if alt_iso3 == iso3:
                    countries.append(alt_name.upper())
        
        return list(set(countries))  # Remove duplicates
    
    def process_csv_for_subregion(self, df: pd.DataFrame, region: str, subregion: str) -> pd.DataFrame:
        """Filter DataFrame for specific sub-region countries"""
        if df.empty or 'PopCountry' not in df.columns:
            return pd.DataFrame()
        
        # Get all country variations for this sub-region
        target_countries = self.get_subregion_countries(region, subregion)
        
        # Find matching rows
        matching_rows = []
        for idx, row in df.iterrows():
            pop_country = str(row.get('PopCountry', '')).strip()
            if not pop_country:
                continue
            
            # Check if this country belongs to our sub-region
            iso3 = self.country_manager.identify_country(pop_country)
            if iso3:
                region_info = self.country_manager.get_region(iso3)
                if region_info and region_info[0] == region and region_info[1] == subregion:
                    matching_rows.append(idx)
        
        if matching_rows:
            return df.loc[matching_rows].copy()
        
        return pd.DataFrame()
    
    def download_file(self, url: str, dest_path: Path) -> bool:
        """Download file with progress"""
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
            
            return True
            
        except Exception as e:
            logger.error(f"Download failed: {e}")
            
            # Try S3 fallback
            if "sam.gov" in url and "s3.amazonaws.com" not in url:
                s3_url = url.replace(
                    "https://sam.gov/api/prod/fileextractservices/v1/api/download/",
                    "https://falextracts.s3.amazonaws.com/"
                ).replace("?privacy=Public", "")
                
                logger.info(f"Trying S3 fallback: {s3_url}")
                return self.download_file(s3_url, dest_path)
            
            return False
    
    def process_year_for_subregion(self, year: int, region: str, subregion: str) -> Tuple[int, int]:
        """Process a single year for a specific sub-region"""
        segment_key = f"{region}_{subregion}_FY{year}"
        
        # Check if already processed
        if segment_key in self.completed_segments:
            if self.completed_segments[segment_key].get('status') == 'completed':
                logger.info(f"  Skipping {segment_key} - already completed")
                return 0, 0
        
        logger.info(f"  Processing FY{year} for {subregion}...")
        
        # Determine source
        if year == datetime.now().year or year == datetime.now().year + 1:
            # Current data
            url = self.config.current_csv_url
            source_key = f"CURRENT_{region}_{subregion}"
        else:
            # Archive data
            url = f"{self.config.archive_base_url}FY{year}_archived_opportunities.csv?privacy=Public"
            source_key = f"FY{year}_{region}_{subregion}"
        
        inserted = 0
        found = 0
        
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / f"FY{year}.csv"
            
            # Download file
            if not self.download_file(url, csv_path):
                logger.warning(f"    Could not download FY{year}")
                self._save_progress(segment_key, "not_found")
                return 0, 0
            
            # Process CSV in chunks
            try:
                chunk_num = 0
                for chunk in pd.read_csv(csv_path, chunksize=self.chunk_size, 
                                        dtype=str, on_bad_lines='skip', low_memory=False):
                    chunk_num += 1
                    
                    # Filter for this sub-region
                    subregion_data = self.process_csv_for_subregion(chunk, region, subregion)
                    
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
                    
                    # Clear memory periodically
                    if chunk_num % 50 == 0:
                        gc.collect()
                
                logger.info(f"    FY{year} complete: {found} found, {inserted} inserted")
                
                # Update statistics
                if region not in self.stats['by_region']:
                    self.stats['by_region'][region] = 0
                self.stats['by_region'][region] += inserted
                
                if subregion not in self.stats['by_subregion']:
                    self.stats['by_subregion'][subregion] = 0
                self.stats['by_subregion'][subregion] += inserted
                
                self.stats['total_processed'] += 1
                self.stats['total_found'] += found
                
                # Save progress
                self._save_progress(segment_key, "completed")
                
            except Exception as e:
                logger.error(f"Error processing {segment_key}: {e}")
                self._save_progress(segment_key, "error")
        
        # Clean up memory
        gc.collect()
        
        return found, inserted
    
    def process_subregion(self, region: str, subregion: str, 
                         start_year: int, end_year: int) -> Dict[str, int]:
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
                        year_increment: int = 5, clear_first: bool = False):
        """
        Run bootstrap for specific sub-regions with year increments
        
        Args:
            target_region: Specific region to process (e.g., 'AFRICA')
            target_subregion: Specific sub-region to process
            start_year: First year to process
            end_year: Last year to process
            year_increment: Process years in chunks of this size
            clear_first: Whether to clear database first
        """
        start_time = datetime.now()
        
        # Determine years
        if end_year is None:
            today = datetime.today()
            end_year = today.year if today.month < 10 else today.year + 1
        
        if start_year is None:
            start_year = end_year - 4  # Default to last 5 years
        
        logger.info("="*60)
        logger.info("Optimized Global SAM.gov Bootstrap")
        logger.info(f"Processing by Sub-Region")
        logger.info(f"Years: FY{start_year} to FY{end_year}")
        logger.info(f"Year Increment: {year_increment}")
        logger.info("="*60)
        
        # Initialize database if needed
        if clear_first:
            self.clear_and_init_database()
        elif not self.config.db_path.exists():
            self.db_manager.initialize_database()
        
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
        logger.info("OPTIMIZED BOOTSTRAP COMPLETE!")
        logger.info("="*60)
        logger.info(f"Time elapsed: {elapsed}")
        logger.info(f"Total segments processed: {self.stats['total_processed']}")
        logger.info(f"Total opportunities found: {self.stats['total_found']:,}")
        
        if self.stats['by_region']:
            logger.info("\n📊 Inserted by region:")
            for region, count in self.stats['by_region'].items():
                logger.info(f"  {region}: {count:,}")
        
        logger.info("\n✅ Bootstrap completed successfully!")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Optimized Global SAM.gov Bootstrap by Sub-Region"
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
    bootstrap = OptimizedGlobalBootstrap()
    
    # Run bootstrap
    bootstrap.run_by_subregion(
        target_region=args.region,
        target_subregion=args.subregion,
        start_year=args.start_year,
        end_year=args.end_year,
        year_increment=args.year_increment,
        clear_first=args.clear
    )


if __name__ == "__main__":
    main()