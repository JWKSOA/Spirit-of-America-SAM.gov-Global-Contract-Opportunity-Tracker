#!/usr/bin/env python3
"""
global_sam_utils.py - Global SAM.gov data handler for Spirit of America
Handles all countries worldwide organized by Spirit of America's portfolio regions
"""

import os
import re
import sqlite3
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import dataclass, field
import json

import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
                "Mauritius": "MUS", "Mayotte": "MYT", "Mozambique": "MOZ", "Réunion": "REU",
                "Rwanda": "RWA", "Seychelles": "SYC", "Somalia": "SOM", "South Sudan": "SSD",
                "Tanzania": "TZA", "Uganda": "UGA", "Zambia": "ZMB", "Zimbabwe": "ZWE",
                "British Indian Ocean Territory": "IOT"
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
                "Nigeria": "NGA", "Senegal": "SEN", "Sierra Leone": "SLE", "Togo": "TGO",
                "Saint Helena": "SHN", "Ascension Island": "SHN", "Tristan da Cunha": "SHN"
            }
        },
        "AMERICAS": {
            "Caribbean": {
                "Anguilla": "AIA", "Antigua and Barbuda": "ATG", "Aruba": "ABW",
                "Bahamas": "BHS", "Barbados": "BRB", "Bonaire": "BES",
                "British Virgin Islands": "VGB", "Cayman Islands": "CYM", "Cuba": "CUB",
                "Curaçao": "CUW", "Dominica": "DMA", "Dominican Republic": "DOM",
                "Grenada": "GRD", "Guadeloupe": "GLP", "Haiti": "HTI", "Jamaica": "JAM",
                "Martinique": "MTQ", "Montserrat": "MSR", "Puerto Rico": "PRI",
                "Saint Barthélemy": "BLM", "Saint Kitts and Nevis": "KNA",
                "Saint Lucia": "LCA", "Saint Martin": "MAF",
                "Saint Vincent and the Grenadines": "VCT", "Sint Maarten": "SXM",
                "Trinidad and Tobago": "TTO", "Turks and Caicos Islands": "TCA",
                "United States Virgin Islands": "VIR"
            },
            "Central America": {
                "Belize": "BLZ", "Costa Rica": "CRI", "El Salvador": "SLV",
                "Guatemala": "GTM", "Honduras": "HND", "Mexico": "MEX",
                "Nicaragua": "NIC", "Panama": "PAN"
            },
            "Northern America": {
                "Bermuda": "BMU", "Canada": "CAN", "Greenland": "GRL",
                "Saint Pierre and Miquelon": "SPM", "United States": "USA"
            },
            "South America": {
                "Argentina": "ARG", "Bolivia": "BOL", "Brazil": "BRA", "Chile": "CHL",
                "Colombia": "COL", "Ecuador": "ECU", "Falkland Islands": "FLK",
                "French Guiana": "GUF", "Guyana": "GUY", "Paraguay": "PRY", "Peru": "PER",
                "Suriname": "SUR", "Uruguay": "URY", "Venezuela": "VEN"
            }
        },
        "ASIA": {
            "Eastern Asia": {
                "China": "CHN", "Hong Kong": "HKG", "Macao": "MAC", "Japan": "JPN",
                "Mongolia": "MNG", "Democratic People's Republic of Korea": "PRK",
                "Republic of Korea": "KOR", "Taiwan": "TWN"
            },
            "Southern Asia": {
                "Afghanistan": "AFG", "Bangladesh": "BGD", "Bhutan": "BTN", "India": "IND",
                "Maldives": "MDV", "Nepal": "NPL", "Pakistan": "PAK", "Sri Lanka": "LKA"
            },
            "South-Eastern Asia": {
                "Brunei Darussalam": "BRN", "Cambodia": "KHM", "Indonesia": "IDN",
                "Lao People's Democratic Republic": "LAO", "Malaysia": "MYS",
                "Myanmar": "MMR", "Philippines": "PHL", "Singapore": "SGP",
                "Thailand": "THA", "Timor-Leste": "TLS", "Vietnam": "VNM"
            },
            "Oceania": {
                "Australia": "AUS", "Christmas Island": "CXR", "Cocos (Keeling) Islands": "CCK",
                "Heard Island and McDonald Islands": "HMD", "Norfolk Island": "NFK",
                "New Zealand": "NZL", "Fiji": "FJI", "New Caledonia": "NCL",
                "Papua New Guinea": "PNG", "Solomon Islands": "SLB", "Vanuatu": "VUT",
                "Guam": "GUM", "Kiribati": "KIR", "Marshall Islands": "MHL",
                "Micronesia (Federated States of)": "FSM", "Nauru": "NRU",
                "Northern Mariana Islands": "MNP", "Palau": "PLW",
                "United States Minor Outlying Islands": "UMI", "American Samoa": "ASM",
                "Cook Islands": "COK", "French Polynesia": "PYF", "Niue": "NIU",
                "Pitcairn": "PCN", "Samoa": "WSM", "Tokelau": "TKL", "Tonga": "TON",
                "Tuvalu": "TUV", "Wallis and Futuna": "WLF"
            }
        },
        "MIDDLE_EAST": {
            "Near-East": {
                "Turkey": "TUR", "Iraq": "IRQ", "Israel": "ISR", "Jordan": "JOR",
                "Kuwait": "KWT", "Lebanon": "LBN", "State of Palestine": "PSE",
                "Syrian Arab Republic": "SYR", "Cyprus": "CYP"
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
                "Hungary": "HUN", "Poland": "POL", "Republic of Moldova": "MDA",
                "Romania": "ROU", "Russian Federation": "RUS", "Slovakia": "SVK",
                "Ukraine": "UKR"
            },
            "Northern Europe": {
                "Åland Islands": "ALA", "Channel Islands (Guernsey)": "GGY",
                "Channel Islands (Jersey)": "JEY", "Denmark": "DNK", "Estonia": "EST",
                "Faroe Islands": "FRO", "Finland": "FIN", "Iceland": "ISL", "Ireland": "IRL",
                "Isle of Man": "IMN", "Latvia": "LVA", "Lithuania": "LTU", "Norway": "NOR",
                "Svalbard and Jan Mayen": "SJM", "Sweden": "SWE", "United Kingdom": "GBR"
            },
            "Southern Europe": {
                "Albania": "ALB", "Andorra": "AND", "Bosnia and Herzegovina": "BIH",
                "Croatia": "HRV", "Gibraltar": "GIB", "Greece": "GRC", "Holy See": "VAT",
                "Italy": "ITA", "Malta": "MLT", "Montenegro": "MNE", "North Macedonia": "MKD",
                "Portugal": "PRT", "San Marino": "SMR", "Serbia": "SRB",
                "Slovenia": "SVN", "Spain": "ESP"
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
        "USA": "USA", "US": "USA", "UNITED STATES OF AMERICA": "USA",
        "UK": "GBR", "BRITAIN": "GBR", "GREAT BRITAIN": "GBR",
        "UAE": "ARE", "EMIRATES": "ARE",
        "DRC": "COD", "DR CONGO": "COD", "CONGO-KINSHASA": "COD",
        "CONGO-BRAZZAVILLE": "COG", "REPUBLIC OF CONGO": "COG",
        "IVORY COAST": "CIV", "COTE DIVOIRE": "CIV",
        "CABO VERDE": "CPV", "CAPE VERDE ISLANDS": "CPV",
        "CZECHIA": "CZE", "CZECH": "CZE",
        "NORTH KOREA": "PRK", "DPRK": "PRK",
        "SOUTH KOREA": "KOR", "ROK": "KOR", "KOREA": "KOR",
        "PALESTINE": "PSE", "PALESTINIAN TERRITORIES": "PSE",
        "SYRIA": "SYR", "IRAN (ISLAMIC REPUBLIC OF)": "IRN",
        "RUSSIA": "RUS", "RUSSIAN FED": "RUS",
        "VIETNAM": "VNM", "VIET NAM": "VNM",
        "LAOS": "LAO", "LAO PDR": "LAO",
        "BRUNEI": "BRN", "MYANMAR (BURMA)": "MMR", "BURMA": "MMR",
        "EAST TIMOR": "TLS", "TIMOR LESTE": "TLS",
        "SWAZILAND": "SWZ", "KINGDOM OF ESWATINI": "SWZ",
        "MACEDONIA": "MKD", "NORTH MACEDONIA (FYROM)": "MKD", "FYROM": "MKD",
        "VATICAN CITY": "VAT", "VATICAN": "VAT", "HOLY SEE (VATICAN CITY)": "VAT",
        "BOSNIA": "BIH", "BOSNIA-HERZEGOVINA": "BIH",
        "MICRONESIA": "FSM", "FSM": "FSM",
        "SAO TOME": "STP", "SAO TOME AND PRINCIPE": "STP",
        "GUINEA BISSAU": "GNB", "GUINEE-BISSAU": "GNB",
        "VIRGIN ISLANDS": "VIR", "US VIRGIN ISLANDS": "VIR", "USVI": "VIR",
        "BVI": "VGB", "BRITISH VIRGIN ISLANDS": "VGB",
        "NETHERLAND ANTILLES": "BES", "DUTCH CARIBBEAN": "BES",
        "ST KITTS": "KNA", "ST LUCIA": "LCA", "ST VINCENT": "VCT",
        "ST MARTIN": "MAF", "SAINT MARTIN (FRENCH)": "MAF",
        "SINT MAARTEN": "SXM", "ST MAARTEN": "SXM", "SAINT MARTIN (DUTCH)": "SXM"
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
        
        # Direct ISO3 match
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
            "EG": "EGY", "KE": "KEN", "SA": "SAU", "AE": "ARE", "IL": "ISR"
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
    
    # SAM.gov column names (same as original)
    sam_columns: Dict[str, str] = field(default_factory=lambda: {
        "NoticeId": "The ID of the notice",
        "Title": "The title of the opportunity",
        "Sol#": "The number of the solicitation",
        "Department/Ind.Agency": "The department (L1)",
        "CGAC": "Common Governmentwide Accounting Classification",
        "Sub-Tier": "The sub-tier (L2)",
        "FPDS Code": "Federal Procurement Data System code",
        "Office": "The office (L3)",
        "AAC Code": "Activity Address Code",
        "PostedDate": "Date posted (YYYY-MM-DD) (HH-MM-SS)",
        "Type": "The opportunity's current type",
        "BaseType": "The opportunity's original type",
        "ArchiveType": "Archive type",
        "ArchiveDate": "Date archived",
        "SetASideCode": "Set aside code",
        "SetASide": "Description of the set aside",
        "ResponseDeadLine": "Deadline date to respond",
        "NaicsCode": "NAICS code",
        "ClassificationCode": "Classification code",
        "PopStreetAddress": "Place of performance street address",
        "PopCity": "Place of performance city",
        "PopState": "Place of performance state",
        "PopZip": "Place of performance zip",
        "PopCountry": "Place of performance country",
        "Active": "If Active = Yes, then opportunity is active",
        "AwardNumber": "The award number",
        "AwardDate": "Date the opportunity was awarded",
        "Award$": "Monetary amount of the award",
        "Awardee": "Name and location of the awardee",
        "PrimaryContactTitle": "Title of the primary contact",
        "PrimaryContactFullName": "Primary contact's full name",
        "PrimaryContactEmail": "Primary contact's email",
        "PrimaryContactPhone": "Primary contact's phone number",
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
                    CGAC TEXT,
                    "Sub-Tier" TEXT,
                    "FPDS Code" TEXT,
                    Office TEXT,
                    "AAC Code" TEXT,
                    PostedDate TEXT,
                    PostedDate_normalized DATE,
                    Type TEXT,
                    BaseType TEXT,
                    ArchiveType TEXT,
                    ArchiveDate TEXT,
                    SetASideCode TEXT,
                    SetASide TEXT,
                    ResponseDeadLine TEXT,
                    NaicsCode TEXT,
                    ClassificationCode TEXT,
                    PopStreetAddress TEXT,
                    PopCity TEXT,
                    PopState TEXT,
                    PopZip TEXT,
                    PopCountry TEXT,
                    PopCountry_ISO3 TEXT,
                    Geographic_Region TEXT,
                    Geographic_SubRegion TEXT,
                    Active TEXT,
                    AwardNumber TEXT,
                    AwardDate TEXT,
                    "Award$" TEXT,
                    Awardee TEXT,
                    PrimaryContactTitle TEXT,
                    PrimaryContactFullName TEXT,
                    PrimaryContactEmail TEXT,
                    PrimaryContactPhone TEXT,
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
                "CREATE INDEX idx_type ON opportunities(Type)",
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
                cur.execute("SELECT PostedDate FROM opportunities WHERE NoticeId = ?", (notice_id,))
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
                    for col in self.config.sam_columns.keys():
                        if col not in ['NoticeId', 'PopCountry'] and col in row.index:
                            if self.needs_quoting(col):
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
                    skipped += 1  # For now, skip updates for simplicity
            
            conn.commit()
        
        logger.info(f"Batch from {source}: {inserted} inserted, {updated} updated, {skipped} skipped")
        return inserted, updated, skipped
    
    def needs_quoting(self, column_name: str) -> bool:
        """Check if column name needs quoting"""
        special_chars = ['/', '#', '$', '-', ' ', '.', '(', ')', '[', ']']
        return any(char in column_name for char in special_chars)
    
    def get_statistics_by_region(self) -> Dict[str, Any]:
        """Get statistics organized by region"""
        stats = {
            'total_records': 0,
            'by_region': {},
            'by_subregion': {},
            'recent_by_region': {}
        }
        
        if not self.db_path.exists():
            return stats
        
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # Total records
                cur.execute("SELECT COUNT(*) FROM opportunities")
                stats['total_records'] = cur.fetchone()[0]
                
                # By region
                cur.execute("""
                    SELECT Geographic_Region, COUNT(*) 
                    FROM opportunities 
                    WHERE Geographic_Region IS NOT NULL 
                    GROUP BY Geographic_Region
                """)
                stats['by_region'] = dict(cur.fetchall())
                
                # By sub-region
                cur.execute("""
                    SELECT Geographic_Region, Geographic_SubRegion, COUNT(*) 
                    FROM opportunities 
                    WHERE Geographic_SubRegion IS NOT NULL 
                    GROUP BY Geographic_Region, Geographic_SubRegion
                """)
                for region, subregion, count in cur.fetchall():
                    if region not in stats['by_subregion']:
                        stats['by_subregion'][region] = {}
                    stats['by_subregion'][region][subregion] = count
                
                # Recent by region (last 30 days)
                cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
                cur.execute("""
                    SELECT Geographic_Region, COUNT(*) 
                    FROM opportunities 
                    WHERE Geographic_Region IS NOT NULL 
                      AND PostedDate_normalized >= ?
                    GROUP BY Geographic_Region
                """, (cutoff,))
                stats['recent_by_region'] = dict(cur.fetchall())
                
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
        
        return stats


# Continue with DataProcessor, HTTPClient, CSVReader classes...
# (These would be similar to your original but work with GlobalCountryManager)

# Note: The rest of the utility classes (DataProcessor, HTTPClient, CSVReader) 
# would be very similar to your original sam_utils.py but using GlobalCountryManager
# instead of AfricanCountryManager