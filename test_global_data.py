#!/usr/bin/env python3
"""
test_global_data.py - Test script to verify data is loading correctly
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_sam_utils import GlobalCountryManager, GlobalConfig, GlobalDatabaseManager


def test_country_identification():
    """Test country identification system"""
    print("\n" + "="*60)
    print("Testing Country Identification")
    print("="*60)
    
    cm = GlobalCountryManager()
    
    # Test various country formats
    test_cases = [
        ("United States", "USA", "AMERICAS"),
        ("USA", "USA", "AMERICAS"),
        ("UK", "GBR", "EUROPE"),
        ("KENYA (KEN)", "KEN", "AFRICA"),
        ("South Africa", "ZAF", "AFRICA"),
        ("Brazil", "BRA", "AMERICAS"),
        ("China", "CHN", "ASIA"),
        ("UAE", "ARE", "MIDDLE_EAST"),
        ("Germany", "DEU", "EUROPE"),
        ("India", "IND", "ASIA"),
        ("Saudi Arabia", "SAU", "MIDDLE_EAST"),
        ("Mexico", "MEX", "AMERICAS"),
        ("Australia", "AUS", "ASIA"),  # In Oceania sub-region
        ("Japan", "JPN", "ASIA"),
        ("Israel", "ISR", "MIDDLE_EAST"),
        ("France", "FRA", "EUROPE"),
        ("Nigeria", "NGA", "AFRICA"),
        ("DRC", "COD", "AFRICA"),
        ("ITA", "ITA", "EUROPE"),  # Raw ISO3 code
        ("CAN", "CAN", "AMERICAS"),  # Raw ISO3 code
    ]
    
    print("\nCountry identification tests:")
    passed = 0
    failed = 0
    
    for test_input, expected_iso3, expected_region in test_cases:
        iso3 = cm.identify_country(test_input)
        
        if iso3 == expected_iso3:
            region_info = cm.get_region(iso3)
            if region_info and region_info[0] == expected_region:
                print(f"  ✅ '{test_input}' -> {iso3} ({expected_region})")
                passed += 1
            else:
                print(f"  ⚠️ '{test_input}' -> {iso3} (region mismatch: expected {expected_region}, got {region_info[0] if region_info else 'None'})")
                failed += 1
        else:
            print(f"  ❌ '{test_input}' -> Expected {expected_iso3}, got {iso3}")
            failed += 1
    
    print(f"\n  Results: {passed} passed, {failed} failed")
    
    # Show region totals
    print("\n📊 Countries per region:")
    for region in cm.GEOGRAPHIC_REGIONS:
        total = sum(len(countries) for countries in cm.GEOGRAPHIC_REGIONS[region].values())
        subregions = len(cm.GEOGRAPHIC_REGIONS[region])
        print(f"  {region}: {total} countries in {subregions} sub-regions")
    
    return passed > 0


def test_database():
    """Test database structure and data"""
    print("\n" + "="*60)
    print("Testing Database")
    print("="*60)
    
    config = GlobalConfig()
    
    if not config.db_path.exists():
        print("\n❌ Database not found!")
        print(f"Expected at: {config.db_path}")
        print("\nRun bootstrap first:")
        print("  python run_global_bootstrap.py")
        return False
    
    print(f"\n✅ Database found at: {config.db_path}")
    print(f"   Size: {config.db_path.stat().st_size / (1024*1024):.2f} MB")
    
    try:
        conn = sqlite3.connect(str(config.db_path))
        cur = conn.cursor()
        
        # Check table structure
        cur.execute("PRAGMA table_info(opportunities)")
        columns = [col[1] for col in cur.fetchall()]
        
        required_columns = [
            'NoticeId', 'Title', 'PopCountry', 'PopCountry_ISO3',
            'Geographic_Region', 'Geographic_SubRegion',
            'PostedDate', 'PostedDate_normalized'
        ]
        
        print("\n📋 Table structure check:")
        for col in required_columns:
            if col in columns:
                print(f"  ✅ Column '{col}' exists")
            else:
                print(f"  ❌ Column '{col}' missing!")
        
        # Get statistics
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total = cur.fetchone()[0]
        print(f"\n📊 Total records: {total:,}")
        
        if total == 0:
            print("\n⚠️ Database is empty. Run bootstrap to load data.")
            conn.close()
            return False
        
        # Records by region
        print("\n🌍 Records by Geographic Region:")
        cur.execute("""
            SELECT Geographic_Region, COUNT(*) as cnt
            FROM opportunities
            WHERE Geographic_Region IS NOT NULL
            GROUP BY Geographic_Region
            ORDER BY cnt DESC
        """)
        
        region_totals = {}
        for region, count in cur.fetchall():
            region_totals[region] = count
            print(f"  {region}: {count:,}")
        
        # Records without region (should be 0 or very few)
        cur.execute("""
            SELECT COUNT(*) FROM opportunities
            WHERE Geographic_Region IS NULL
        """)
        no_region = cur.fetchone()[0]
        if no_region > 0:
            print(f"\n⚠️ Records without region: {no_region:,}")
        
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
            
            # Calculate date range
            earliest_date = datetime.strptime(earliest, '%Y-%m-%d')
            latest_date = datetime.strptime(latest, '%Y-%m-%d')
            days_range = (latest_date - earliest_date).days
            print(f"  Range: {days_range:,} days (~{days_range/365:.1f} years)")
        
        # Recent activity (last 30 days)
        print("\n📈 Recent Activity (Last 30 Days):")
        cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        cur.execute("""
            SELECT Geographic_Region, COUNT(*) as cnt
            FROM opportunities
            WHERE PostedDate_normalized >= ?
              AND Geographic_Region IS NOT NULL
            GROUP BY Geographic_Region
            ORDER BY cnt DESC
        """, (cutoff,))
        
        recent_data = cur.fetchall()
        if recent_data:
            for region, count in recent_data:
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
                Geographic_SubRegion,
                PostedDate_normalized
            FROM opportunities
            WHERE Title IS NOT NULL
            ORDER BY PostedDate_normalized DESC
            LIMIT 5
        """)
        
        for record in cur.fetchall():
            notice_id, title, country, region, subregion, posted = record
            title_short = title[:60] + "..." if len(title) > 60 else title
            print(f"\n  Notice ID: {notice_id}")
            print(f"  Title: {title_short}")
            print(f"  Country: {country}")
            print(f"  Region: {region} - {subregion}")
            print(f"  Posted: {posted}")
        
        # Data quality check
        print("\n🔍 Data Quality Check:")
        
        # Records with ISO3
        cur.execute("""
            SELECT COUNT(*) FROM opportunities
            WHERE PopCountry_ISO3 IS NOT NULL
        """)
        with_iso3 = cur.fetchone()[0]
        print(f"  Records with ISO3: {with_iso3:,} ({with_iso3*100/total:.1f}%)")
        
        # Records with normalized date
        cur.execute("""
            SELECT COUNT(*) FROM opportunities
            WHERE PostedDate_normalized IS NOT NULL
        """)
        with_date = cur.fetchone()[0]
        print(f"  Records with normalized date: {with_date:,} ({with_date*100/total:.1f}%)")
        
        # Active opportunities
        cur.execute("""
            SELECT COUNT(*) FROM opportunities
            WHERE Active = 'Yes'
        """)
        active = cur.fetchone()[0]
        print(f"  Active opportunities: {active:,} ({active*100/total:.1f}%)")
        
        conn.close()
        
        # Check if all regions have data
        print("\n✅ Data Coverage Check:")
        expected_regions = ['AFRICA', 'AMERICAS', 'ASIA', 'MIDDLE_EAST', 'EUROPE']
        missing_regions = []
        for region in expected_regions:
            if region not in region_totals or region_totals[region] == 0:
                missing_regions.append(region)
                print(f"  ⚠️ No data for {region}")
            else:
                print(f"  ✅ {region} has data")
        
        if missing_regions:
            print(f"\n⚠️ Missing data for {len(missing_regions)} region(s)")
            print("Consider running bootstrap for all regions")
        
        return total > 0
        
    except Exception as e:
        print(f"\n❌ Error testing database: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\n" + "="*70)
    print(" 🧪 Spirit of America Global SAM.gov Data Test Suite")
    print("="*70)
    
    # Test country identification
    country_test = test_country_identification()
    
    # Test database
    db_test = test_database()
    
    # Summary
    print("\n" + "="*70)
    print(" Test Summary")
    print("="*70)
    
    if country_test:
        print("✅ Country identification: PASSED")
    else:
        print("❌ Country identification: FAILED")
    
    if db_test:
        print("✅ Database: DATA FOUND")
    else:
        print("⚠️ Database: NO DATA or ERRORS")
    
    if country_test and db_test:
        print("\n🎉 All tests passed! Your system is ready.")
        print("\nYou can now run:")
        print("  streamlit run global_streamlit_dashboard.py")
    elif country_test and not db_test:
        print("\n⚠️ Country system works but no data found.")
        print("\nRun bootstrap first:")
        print("  python run_global_bootstrap.py")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()