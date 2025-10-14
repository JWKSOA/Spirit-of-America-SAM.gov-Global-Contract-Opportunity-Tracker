#!/usr/bin/env python3
"""
test_data_loading.py - Test script to verify data is loading correctly
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_sam_utils import GlobalCountryManager, GlobalConfig, GlobalDatabaseManager

def test_database():
    """Test database and show statistics"""
    print("="*60)
    print("Testing Global SAM.gov Database")
    print("="*60)
    
    config = GlobalConfig()
    country_manager = GlobalCountryManager()
    db_manager = GlobalDatabaseManager(config, country_manager)
    
    # Check if database exists
    if not config.db_path.exists():
        print("\n❌ Database not found!")
        print(f"Expected at: {config.db_path}")
        print("\nPlease run bootstrap first:")
        print("  python run_bootstrap_by_region.py")
        return
    
    print(f"\n✅ Database found at: {config.db_path}")
    print(f"Size: {config.db_path.stat().st_size / (1024*1024):.2f} MB")
    
    # Get statistics
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            
            # Total records
            cur.execute("SELECT COUNT(*) FROM opportunities")
            total = cur.fetchone()[0]
            print(f"\n📊 Total Records: {total:,}")
            
            if total == 0:
                print("\n⚠️ Database is empty. Please run bootstrap.")
                return
            
            # Records by region
            print("\n🌍 Records by Geographic Region:")
            cur.execute("""
                SELECT Geographic_Region, COUNT(*) as cnt
                FROM opportunities
                WHERE Geographic_Region IS NOT NULL
                GROUP BY Geographic_Region
                ORDER BY cnt DESC
            """)
            
            for region, count in cur.fetchall():
                print(f"  {region}: {count:,}")
            
            # Top sub-regions
            print("\n📍 Top 10 Sub-Regions:")
            cur.execute("""
                SELECT Geographic_Region, Geographic_SubRegion, COUNT(*) as cnt
                FROM opportunities
                WHERE Geographic_SubRegion IS NOT NULL
                GROUP BY Geographic_Region, Geographic_SubRegion
                ORDER BY cnt DESC
                LIMIT 10
            """)
            
            for region, subregion, count in cur.fetchall():
                print(f"  {region} - {subregion}: {count:,}")
            
            # Top countries
            print("\n🏆 Top 15 Countries:")
            cur.execute("""
                SELECT PopCountry, COUNT(*) as cnt
                FROM opportunities
                WHERE PopCountry IS NOT NULL
                GROUP BY PopCountry
                ORDER BY cnt DESC
                LIMIT 15
            """)
            
            for country, count in cur.fetchall():
                if country:
                    # Truncate long country names
                    display_name = country[:40] + "..." if len(country) > 40 else country
                    print(f"  {display_name}: {count:,}")
            
            # Date range
            print("\n📅 Date Range:")
            cur.execute("""
                SELECT 
                    MIN(PostedDate_normalized) as earliest,
                    MAX(PostedDate_normalized) as latest
                FROM opportunities
                WHERE PostedDate_normalized IS NOT NULL
            """)
            
            earliest, latest = cur.fetchone()
            if earliest and latest:
                print(f"  Earliest: {earliest}")
                print(f"  Latest: {latest}")
            
            # Recent activity
            print("\n📈 Recent Activity (Last 30 Days):")
            cur.execute("""
                SELECT Geographic_Region, COUNT(*) as cnt
                FROM opportunities
                WHERE PostedDate_normalized >= date('now', '-30 days')
                  AND Geographic_Region IS NOT NULL
                GROUP BY Geographic_Region
                ORDER BY cnt DESC
            """)
            
            recent = cur.fetchall()
            if recent:
                for region, count in recent:
                    print(f"  {region}: {count:,}")
            else:
                print("  No recent activity found")
            
            # Sample records
            print("\n📋 Sample Records (5 most recent):")
            cur.execute("""
                SELECT 
                    NoticeId,
                    Title,
                    PopCountry,
                    Geographic_Region,
                    PostedDate_normalized
                FROM opportunities
                WHERE Title IS NOT NULL
                ORDER BY PostedDate_normalized DESC
                LIMIT 5
            """)
            
            for notice_id, title, country, region, posted in cur.fetchall():
                title_short = title[:50] + "..." if len(title) > 50 else title
                print(f"\n  ID: {notice_id}")
                print(f"  Title: {title_short}")
                print(f"  Country: {country}")
                print(f"  Region: {region}")
                print(f"  Posted: {posted}")
            
            # Check for data issues
            print("\n🔍 Data Quality Check:")
            
            # Records without region
            cur.execute("""
                SELECT COUNT(*) 
                FROM opportunities
                WHERE Geographic_Region IS NULL
            """)
            no_region = cur.fetchone()[0]
            print(f"  Records without region: {no_region:,}")
            
            # Records without ISO3
            cur.execute("""
                SELECT COUNT(*) 
                FROM opportunities
                WHERE PopCountry_ISO3 IS NULL
            """)
            no_iso3 = cur.fetchone()[0]
            print(f"  Records without ISO3: {no_iso3:,}")
            
            # Records without normalized date
            cur.execute("""
                SELECT COUNT(*) 
                FROM opportunities
                WHERE PostedDate_normalized IS NULL
                  AND PostedDate IS NOT NULL
            """)
            no_date = cur.fetchone()[0]
            print(f"  Records without normalized date: {no_date:,}")
            
    except Exception as e:
        print(f"\n❌ Error reading database: {e}")
        print("\nThis might mean the database structure is incorrect.")
        print("Try clearing and re-running bootstrap:")
        print("  python global_bootstrap_optimized.py --clear --quick")
    
    print("\n" + "="*60)
    print("Test Complete!")
    print("="*60)

def test_country_manager():
    """Test country identification"""
    print("\n" + "="*60)
    print("Testing Country Identification")
    print("="*60)
    
    cm = GlobalCountryManager()
    
    # Test various country formats
    test_cases = [
        "United States",
        "USA",
        "US",
        "KENYA (KEN)",
        "South Africa",
        "UAE",
        "United Arab Emirates",
        "Congo-Kinshasa",
        "DRC",
        "UK",
        "Great Britain",
        "Korea",
        "South Korea",
        "FYROM",
        "North Macedonia"
    ]
    
    print("\nCountry identification tests:")
    for test in test_cases:
        iso3 = cm.identify_country(test)
        if iso3:
            region_info = cm.get_region(iso3)
            if region_info:
                region, subregion = region_info
                print(f"  '{test}' -> {iso3} ({region} - {subregion})")
            else:
                print(f"  '{test}' -> {iso3} (no region found)")
        else:
            print(f"  '{test}' -> Not recognized")
    
    # Show region counts
    print("\n📊 Countries per region:")
    for region in cm.GEOGRAPHIC_REGIONS:
        total = sum(len(countries) for countries in cm.GEOGRAPHIC_REGIONS[region].values())
        print(f"  {region}: {total} countries")

if __name__ == "__main__":
    print("\n🔧 SAM.gov Global Database Test Suite\n")
    
    # Test country manager
    test_country_manager()
    
    # Test database
    test_database()
    
    print("\n✅ All tests completed!")
    print("\nIf you see issues above, try running:")
    print("  python run_bootstrap_by_region.py")
    print("\nThen run the Streamlit dashboard:")
    print("  streamlit run global_streamlit_dashboard_improved.py")