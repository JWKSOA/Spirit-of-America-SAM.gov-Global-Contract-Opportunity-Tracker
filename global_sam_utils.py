#!/usr/bin/env python3
"""
global_sam_utils.py - Fixed Global SAM.gov data handler for Spirit of America
Fixed CSV reading based on working Africa Dashboard code
"""

import os
import re
import sqlite3
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import dataclass, field
import json

import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CSV READER - FIXED FROM AFRICA DASHBOARD
# ============================================================================

class CSVReader:
    """Read SAM.gov CSV files with proper encoding handling"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        
    def read_csv_chunks(self, filepath: Path, chunksize: int = None):
        """Read CSV in chunks with encoding detection - FIXED VERSION"""
        if chunksize is None:
            chunksize = self.chunk_size
            
        # Try different encodings in order of likelihood
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                logger.info(f"Reading CSV with encoding: {encoding}")
                
                # Read with all columns as strings to avoid type issues
                for chunk in pd.read_csv(
                    filepath,
                    encoding=encoding,
                    dtype=str,
                    chunksize=chunksize,
                    on_bad_lines='skip',
                    low_memory=False
                ):
                    yield chunk
                    
                return  # Success
                
            except UnicodeDecodeError:
                logger.warning(f"Failed with encoding {encoding}, trying next...")
                continue
            except Exception as e:
                logger.error(f"Error reading CSV with {encoding}: {e}")
                continue
                
        # If all encodings fail, use latin-1 with error handling
        logger.warning("All encodings failed, using latin-1 with error='replace'")
        try:
            for chunk in pd.read_csv(
                filepath,
                encoding='latin-1',
                encoding_errors='replace',
                dtype=str,
                chunksize=chunksize,
                on_bad_lines='skip'
            ):
                yield chunk
        except Exception as e:
            raise ValueError(f"Could not read CSV file: {filepath}. Error: {e}")

# ============================================================================
# GLOBAL COUNTRY DATA - Spirit of America Portfolios
# ============================================================================

class GlobalCountryManager:
    """Manages identification of all countries organized by Spirit of America portfolios"""
    
    # Spirit of America Geographic Portfolios
    GEOGRAPHIC_REGIONS = {
        "AFRICA": {
            "Northern Africa": {
                "Algeria": "DZA", "Egypt": "EGY", "Libya": "LBY", "Mauritania": "MRT",
                "Morocco": "MAR", "Sudan": "SDN", "Tunisia": "TUN", "Western Sahara": "ESH"
            },
            "Eastern Africa": {
                "Burundi": "BDI", "Comoros": "COM", "Djibouti": "DJI", "Eritrea": "ERI",
                "Ethiopia": "ETH", "Kenya": "KEN", "Madagascar": "MDG", "Malawi": "MWI",
                "Mauritius": "MUS", "Mozambique": "MOZ", "Rwanda": "RWA", 
                "Seychelles": "SYC", "Somalia": "SOM", "South Sudan": "SSD",
                "Tanzania": "TZA", "Uganda": "UGA", "Zambia": "ZMB", "Zimbabwe": "ZWE"
            },
            "Middle Africa": {
                "Angola": "AGO", "Cameroon": "CMR", "Central African Republic": "CAF",
                "Chad": "TCD", "Congo": "COG", "Democratic Republic of the Congo": "COD",
                "Equatorial Guinea": "GNQ", "Gabon": "GAB", "São Tomé and Príncipe": "STP"
            },
            "Southern Africa": {
                "Botswana": "BWA", "Eswatini": "SWZ", "Lesotho": "LSO",
                "Namibia": "NAM", "South Africa": "ZAF"
            },
            "Western Africa": {
                "Benin": "BEN", "Burkina Faso": "BFA", "Cabo Verde": "CPV",
                "Côte d'Ivoire": "CIV", "Gambia": "GMB", "Ghana": "GHA", "Guinea": "GIN",
                "Guinea-Bissau": "GNB", "Liberia": "LBR", "Mali": "MLI", "Niger": "NER",
                "Nigeria": "NGA", "Senegal": "SEN", "Sierra Leone": "SLE", "Togo": "TGO"
            }
        },
        "AMERICAS": {
            "Caribbean": {
                "Antigua and Barbuda": "ATG", "Aruba": "ABW",
                "Bahamas": "BHS", "Barbados": "BRB", "Cuba": "CUB",
                "Curaçao": "CUW", "Dominica": "DMA", "Dominican Republic": "DOM",
                "Grenada": "GRD", "Haiti": "HTI", "Jamaica": "JAM",
                "Puerto Rico": "PRI", "Saint Kitts and Nevis": "KNA",
                "Saint Lucia": "LCA", "Saint Vincent and the Grenadines": "VCT",
                "Trinidad and Tobago": "TTO"
            },
            "Central America": {
                "Belize": "BLZ", "Costa Rica": "CRI", "El Salvador": "SLV",
                "Guatemala": "GTM", "Honduras": "HND", "Mexico": "MEX",
                "Nicaragua": "NIC", "Panama": "PAN"
            },
            "Northern America": {
                "Canada": "CAN", "United States": "USA"
            },
            "South America": {
                "Argentina": "ARG", "Bolivia": "BOL", "Brazil": "BRA", "Chile": "CHL",
                "Colombia": "COL", "Ecuador": "ECU", "French Guiana": "GUF", 
                "Guyana": "GUY", "Paraguay": "PRY", "Peru": "PER",
                "Suriname": "SUR", "Uruguay": "URY", "Venezuela": "VEN"
            }
        },
        "ASIA": {
            "Eastern Asia": {
                "China": "CHN", "Hong Kong": "HKG", "Japan": "JPN",
                "Mongolia": "MNG", "North Korea": "PRK",
                "South Korea": "KOR", "Taiwan": "TWN"
            },
            "Southern Asia": {
                "Afghanistan": "AFG", "Bangladesh": "BGD", "Bhutan": "BTN", "India": "IND",
                "Maldives": "MDV", "Nepal": "NPL", "Pakistan": "PAK", "Sri Lanka": "LKA"
            },
            "South-Eastern Asia": {
                "Brunei": "BRN", "Cambodia": "KHM", "Indonesia": "IDN",
                "Laos": "LAO", "Malaysia": "MYS",
                "Myanmar": "MMR", "Philippines": "PHL", "Singapore": "SGP",
                "Thailand": "THA", "Timor-Leste": "TLS", "Vietnam": "VNM"
            },
            "Oceania": {
                "Australia": "AUS", "New Zealand": "NZL", "Fiji": "FJI",
                "Papua New Guinea": "PNG", "Solomon Islands": "SLB", "Vanuatu": "VUT",
                "Kiribati": "KIR", "Marshall Islands": "MHL",
                "Micronesia": "FSM", "Nauru": "NRU", "Palau": "PLW",
                "Samoa": "WSM", "Tonga": "TON", "Tuvalu": "TUV"
            }
        },
        "MIDDLE_EAST": {
            "Near-East": {
                "Turkey": "TUR", "Iraq": "IRQ", "Israel": "ISR", "Jordan": "JOR",
                "Kuwait": "KWT", "Lebanon": "LBN", "Palestine": "PSE",
                "Syria": "SYR", "Cyprus": "CYP"
            },
            "Far-East": {
                "Armenia": "ARM", "Azerbaijan": "AZE", "Bahrain": "BHR", "Georgia": "GEO",
                "Iran": "IRN", "Oman": "OMN", "Qatar": "QAT", "Saudi Arabia": "SAU",
                "United Arab Emirates": "ARE", "Yemen": "YEM", "Kazakhstan": "KAZ",
                "Kyrgyzstan": "KGZ", "Tajikistan": "TJK", "Turkmenistan": "TKM",
                "Uzbekistan": "UZB"
            }
        },
        "EUROPE": {
            "Eastern Europe": {
                "Belarus": "BLR", "Bulgaria": "BGR", "Czech Republic": "CZE",
                "Hungary": "HUN", "Poland": "POL", "Moldova": "MDA",
                "Romania": "ROU", "Russia": "RUS", "Slovakia": "SVK",
                "Ukraine": "UKR"
            },
            "Northern Europe": {
                "Denmark": "DNK", "Estonia": "EST", "Finland": "FIN", 
                "Iceland": "ISL", "Ireland": "IRL", "Latvia": "LVA", 
                "Lithuania": "LTU", "Norway": "NOR", "Sweden": "SWE", 
                "United Kingdom": "GBR"
            },
            "Southern Europe": {
                "Albania": "ALB", "Andorra": "AND", "Bosnia and Herzegovina": "BIH",
                "Croatia": "HRV", "Greece": "GRC", "Italy": "ITA", 
                "Malta": "MLT", "Montenegro": "MNE", "North Macedonia": "MKD",
                "Portugal": "PRT", "San Marino": "SMR", "Serbia": "SRB",
                "Slovenia": "SVN", "Spain": "ESP", "Vatican City": "VAT"
            },
            "Western Europe": {
                "Austria": "AUT", "Belgium": "BEL", "France": "FRA", "Germany": "DEU",
                "Liechtenstein": "LIE", "Luxembourg": "LUX", "Monaco": "MCO",
                "Netherlands": "NLD", "Switzerland": "CHE"
            }
        }
    }
    
    # Alternative names and variations
    ALTERNATIVE_NAMES = {
        # Common variations
        "USA": "USA", "US": "USA", "UNITED STATES OF AMERICA": "USA", "UNITED STATES": "USA",
        "UK": "GBR", "BRITAIN": "GBR", "GREAT BRITAIN": "GBR", "ENGLAND": "GBR",
        "UAE": "ARE", "EMIRATES": "ARE",
        "DRC": "COD", "DR CONGO": "COD", "CONGO-KINSHASA": "COD", "CONGO KINSHASA": "COD",
        "CONGO-BRAZZAVILLE": "COG", "REPUBLIC OF CONGO": "COG", "CONGO BRAZZAVILLE": "COG",
        "IVORY COAST": "CIV", "COTE DIVOIRE": "CIV", "COTE D'IVOIRE": "CIV",
        "CABO VERDE": "CPV", "CAPE VERDE": "CPV", "CAPE VERDE ISLANDS": "CPV",
        "CZECHIA": "CZE", "CZECH": "CZE",
        "NORTH KOREA": "PRK", "DPRK": "PRK", "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA": "PRK",
        "SOUTH KOREA": "KOR", "ROK": "KOR", "KOREA": "KOR", "REPUBLIC OF KOREA": "KOR",
        "PALESTINE": "PSE", "PALESTINIAN TERRITORIES": "PSE", "STATE OF PALESTINE": "PSE",
        "SYRIA": "SYR", "SYRIAN ARAB REPUBLIC": "SYR",
        "RUSSIA": "RUS", "RUSSIAN FED": "RUS", "RUSSIAN FEDERATION": "RUS",
        "VIETNAM": "VNM", "VIET NAM": "VNM",
        "LAOS": "LAO", "LAO PDR": "LAO", "LAO PEOPLE'S DEMOCRATIC REPUBLIC": "LAO",
        "BRUNEI": "BRN", "BRUNEI DARUSSALAM": "BRN",
        "MYANMAR": "MMR", "BURMA": "MMR",
        "TIMOR-LESTE": "TLS", "EAST TIMOR": "TLS", "TIMOR LESTE": "TLS",
        "MACEDONIA": "MKD", "NORTH MACEDONIA": "MKD", "FYROM": "MKD",
        "SWAZILAND": "SWZ", "KINGDOM OF ESWATINI": "SWZ", "ESWATINI": "SWZ",
        "MICRONESIA": "FSM", "FEDERATED STATES OF MICRONESIA": "FSM",
        "SAO TOME": "STP", "SAO TOME AND PRINCIPE": "STP", "SÃO TOMÉ AND PRÍNCIPE": "STP",
        "NETHERLANDS": "NLD", "HOLLAND": "NLD",
        "BOSNIA": "BIH", "BOSNIA-HERZEGOVINA": "BIH",
        "VATICAN": "VAT", "VATICAN CITY": "VAT", "HOLY SEE": "VAT"
    }
    
    def __init__(self):
        # Build lookup structures
        self.all_countries = {}  # Full country name -> ISO3
        self.iso3_to_country = {}  # ISO3 -> Full country name
        self.iso3_to_region = {}  # ISO3 -> (region, sub_region)
        self.region_countries = {}  # Region -> list of ISO3 codes
        self.subregion_countries = {}  # (Region, SubRegion) -> list of ISO3 codes
        
        # Process all regions
        for region, subregions in self.GEOGRAPHIC_REGIONS.items():
            self.region_countries[region] = []
            
            for subregion, countries in subregions.items():
                key = (region, subregion)
                self.subregion_countries[key] = []
                
                for country_name, iso3 in countries.items():
                    # Store mappings
                    self.all_countries[country_name.upper()] = iso3
                    self.iso3_to_country[iso3] = country_name
                    self.iso3_to_region[iso3] = (region, subregion)
                    self.region_countries[region].append(iso3)
                    self.subregion_countries[key].append(iso3)
        
        # Add alternative names
        for alt_name, iso3 in self.ALTERNATIVE_NAMES.items():
            self.all_countries[alt_name.upper()] = iso3
        
        # Create set of all valid ISO3 codes
        self.all_iso3_codes = set(self.iso3_to_country.keys())
    
    def identify_country(self, value: str) -> Optional[str]:
        """
        Identify country from various formats and return ISO3 code
        Returns None if not a recognized country
        """
        if not value or pd.isna(value) or value == '':
            return None
        
        value_clean = str(value).upper().strip()
        
        # Skip non-country values
        if value_clean in ['NONE', 'NULL', 'N/A', 'UNKNOWN', '']:
            return None
        
        # Direct ISO3 match (e.g., "USA", "GBR", "FRA")
        if len(value_clean) == 3 and value_clean.isalpha() and value_clean in self.all_iso3_codes:
            return value_clean
        
        # Direct country name match
        if value_clean in self.all_countries:
            return self.all_countries[value_clean]
        
        # Extract ISO from parentheses (e.g., "KENYA (KEN)")
        if '(' in value_clean and ')' in value_clean:
            iso_match = re.search(r'\(([A-Z]{3})\)', value_clean)
            if iso_match and iso_match.group(1) in self.all_iso3_codes:
                return iso_match.group(1)
        
        # Check for ISO2 codes (convert common ones)
        iso2_to_iso3 = {
            "US": "USA", "GB": "GBR", "FR": "FRA", "DE": "DEU", "IT": "ITA",
            "ES": "ESP", "CA": "CAN", "AU": "AUS", "JP": "JPN", "CN": "CHN",
            "IN": "IND", "BR": "BRA", "MX": "MEX", "ZA": "ZAF", "NG": "NGA",
            "EG": "EGY", "KE": "KEN", "SA": "SAU", "AE": "ARE", "IL": "ISR",
            "TR": "TUR", "PL": "POL", "NL": "NLD", "BE": "BEL", "CH": "CHE"
        }
        if len(value_clean) == 2 and value_clean in iso2_to_iso3:
            return iso2_to_iso3[value_clean]
        
        # Partial match for longer strings
        if len(value_clean) > 3:
            for country_name, iso3 in self.all_countries.items():
                if country_name in value_clean or value_clean in country_name:
                    return iso3
        
        return None
    
    def get_region(self, iso3: str) -> Optional[Tuple[str, str]]:
        """Get region and sub-region for an ISO3 code"""
        return self.iso3_to_region.get(iso3)
    
    def standardize_country(self, value: str) -> Optional[str]:
        """
        Standardize country to 'COUNTRY NAME (ISO3)' format
        Returns None if not a recognized country
        """
        iso3 = self.identify_country(value)
        if iso3 and iso3 in self.iso3_to_country:
            country_name = self.iso3_to_country[iso3]
            return f"{country_name} ({iso3})"
        return None
    
    def get_countries_by_region(self, region: str) -> List[str]:
        """Get all ISO3 codes for a specific region"""
        return self.region_countries.get(region, [])
    
    def get_countries_by_subregion(self, region: str, subregion: str) -> List[str]:
        """Get all ISO3 codes for a specific sub-region"""
        return self.subregion_countries.get((region, subregion), [])


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class GlobalConfig:
    """Configuration for global SAM.gov data processing"""
    
    # Database location
    db_path: Path = field(default_factory=lambda: Path("data") / "global_opportunities.db")
    data_dir: Path = field(default_factory=lambda: Path("data"))
    cache_dir: Path = field(default_factory=lambda: Path("data") / ".cache")
    
    # Processing
    chunk_size: int = 10000
    max_retries: int = 3
    timeout_seconds: int = 300
    
    # SAM.gov URLs
    current_csv_url: str = (
        "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
        "Contract%20Opportunities/datagov/ContractOpportunitiesFullCSV.csv?privacy=Public"
    )
    
    archive_base_url: str = (
        "https://sam.gov/api/prod/fileextractservices/v1/api/download/"
        "Contract%20Opportunities/Archived%20Data/"
    )
    
    # Alternative S3 URLs (fallback)
    s3_current_url: str = (
        "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/datagov/"
        "ContractOpportunitiesFullCSV.csv"
    )
    
    s3_archive_base: str = (
        "https://falextracts.s3.amazonaws.com/Contract%20Opportunities/Archived%20Data/"
    )
    
    # SAM.gov column names
    sam_columns: Dict[str, str] = field(default_factory=lambda: {
        "NoticeId": "The ID of the notice",
        "Title": "The title of the opportunity",
        "Sol#": "The number of the solicitation",
        "Department/Ind.Agency": "The department (L1)",
        "PostedDate": "Date posted (YYYY-MM-DD) (HH-MM-SS)",
        "Type": "The opportunity's current type",
        "PopCountry": "Place of performance country",
        "PopCity": "Place of performance city",
        "PopState": "Place of performance state",
        "Active": "If Active = Yes, then opportunity is active",
        "ResponseDeadLine": "Deadline date to respond",
        "Link": "The direct UI link to the opportunity",
        "Description": "Description of the opportunity"
    })
    
    def __post_init__(self):
        """Create necessary directories"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)


# ============================================================================
# DATABASE MANAGER - Extended with region columns
# ============================================================================

class GlobalDatabaseManager:
    """Database operations for global SAM.gov data with region tracking"""
    
    def __init__(self, config: GlobalConfig, country_manager: GlobalCountryManager):
        self.config = config
        self.db_path = config.db_path
        self.country_manager = country_manager
    
    @contextmanager
    def get_connection(self):
        """Get database connection with optimizations"""
        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA temp_store=MEMORY")
            conn.execute("PRAGMA cache_size=10000")
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def initialize_database(self):
        """Create database with SAM.gov schema plus region fields"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Drop existing table to start fresh
            cur.execute("DROP TABLE IF EXISTS opportunities")
            
            # Create table with SAM.gov columns plus region tracking
            cur.execute("""
                CREATE TABLE opportunities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    NoticeId TEXT UNIQUE NOT NULL,
                    Title TEXT,
                    "Sol#" TEXT,
                    "Department/Ind.Agency" TEXT,
                    PostedDate TEXT,
                    PostedDate_normalized DATE,
                    Type TEXT,
                    PopCountry TEXT,
                    PopCountry_ISO3 TEXT,
                    Geographic_Region TEXT,
                    Geographic_SubRegion TEXT,
                    PopCity TEXT,
                    PopState TEXT,
                    Active TEXT,
                    ResponseDeadLine TEXT,
                    Link TEXT,
                    Description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for performance
            indexes = [
                "CREATE INDEX idx_notice_id ON opportunities(NoticeId)",
                "CREATE INDEX idx_posted_date ON opportunities(PostedDate)",
                "CREATE INDEX idx_posted_norm ON opportunities(PostedDate_normalized)",
                "CREATE INDEX idx_pop_country ON opportunities(PopCountry)",
                "CREATE INDEX idx_pop_iso3 ON opportunities(PopCountry_ISO3)",
                "CREATE INDEX idx_region ON opportunities(Geographic_Region)",
                "CREATE INDEX idx_subregion ON opportunities(Geographic_SubRegion)",
                "CREATE INDEX idx_active ON opportunities(Active)",
                "CREATE INDEX idx_region_date ON opportunities(Geographic_Region, PostedDate_normalized DESC)"
            ]
            
            for idx_sql in indexes:
                cur.execute(idx_sql)
            
            conn.commit()
            logger.info("Global database initialized with region tracking")
    
    def normalize_posted_date(self, date_str: str) -> Optional[str]:
        """Normalize PostedDate from SAM.gov format to YYYY-MM-DD"""
        if not date_str or pd.isna(date_str) or date_str == '':
            return None
        
        date_str = str(date_str).strip()
        
        # Already normalized
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
        
        # SAM.gov format with time
        if ' ' in date_str:
            date_part = date_str.split(' ')[0]
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
                return date_part
        
        # Try pandas parsing as fallback
        try:
            parsed = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(parsed):
                return parsed.strftime('%Y-%m-%d')
        except:
            pass
        
        return None
    
    def insert_or_update_batch(self, df: pd.DataFrame, source: str = "unknown") -> Tuple[int, int, int]:
        """Insert or update batch with region identification"""
        if df.empty:
            return 0, 0, 0
        
        inserted = 0
        updated = 0
        skipped = 0
        
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            for _, row in df.iterrows():
                notice_id = str(row.get('NoticeId', '')).strip()
                if not notice_id or notice_id in ['nan', 'None', '']:
                    skipped += 1
                    continue
                
                # Identify country and region
                pop_country = row.get('PopCountry', '')
                iso3 = self.country_manager.identify_country(pop_country)
                
                # Skip if country not recognized
                if not iso3:
                    skipped += 1
                    continue
                
                # Get region info
                region_info = self.country_manager.get_region(iso3)
                if region_info:
                    region, subregion = region_info
                else:
                    region, subregion = None, None
                
                # Check if exists
                cur.execute("SELECT id FROM opportunities WHERE NoticeId = ?", (notice_id,))
                existing = cur.fetchone()
                
                if not existing:
                    # Insert new record
                    columns = ['NoticeId', 'PopCountry_ISO3', 'Geographic_Region', 'Geographic_SubRegion']
                    values = [notice_id, iso3, region, subregion]
                    
                    # Add normalized date
                    posted_date = row.get('PostedDate', '')
                    normalized_date = self.normalize_posted_date(posted_date)
                    columns.append('PostedDate_normalized')
                    values.append(normalized_date)
                    
                    # Add standardized country name
                    std_country = self.country_manager.standardize_country(pop_country)
                    columns.append('PopCountry')
                    values.append(std_country)
                    
                    # Add other columns
                    for col in ['Title', 'Department/Ind.Agency', 'PostedDate', 'Type', 
                               'PopCity', 'PopState', 'Active', 'ResponseDeadLine', 
                               'Link', 'Description', 'Sol#']:
                        if col in row.index:
                            if col == 'Department/Ind.Agency' or col == 'Sol#':
                                columns.append(f'"{col}"')
                            else:
                                columns.append(col)
                            values.append(row[col] if pd.notna(row[col]) else None)
                    
                    # Execute insert
                    placeholders = ','.join(['?' for _ in values])
                    columns_str = ','.join(columns)
                    
                    try:
                        cur.execute(f"INSERT OR IGNORE INTO opportunities ({columns_str}) VALUES ({placeholders})", values)
                        if cur.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1
                    except Exception as e:
                        logger.error(f"Insert error for {notice_id}: {e}")
                        skipped += 1
                else:
                    skipped += 1  # For simplicity, skip updates
            
            conn.commit()
        
        if inserted > 0 or updated > 0:
            logger.info(f"Batch from {source}: {inserted} inserted, {updated} updated, {skipped} skipped")
        
        return inserted, updated, skipped