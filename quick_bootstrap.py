#!/usr/bin/env python3
"""
quick_bootstrap.py - Quick bootstrap for last 2 years of data
Simple script to get your dashboard up and running quickly
"""

import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_bootstrap import GlobalBootstrap

def main():
    """Quick bootstrap for recent data"""
    print("\n" + "="*60)
    print("🚀 QUICK BOOTSTRAP - Last 2 Years of Data")
    print("="*60)
    print("\nThis will download the most recent contract opportunities")
    print("for all countries worldwide, organized by Spirit of America regions.")
    print("\nEstimated time: 10-15 minutes")
    print("="*60)
    
    # Ask for confirmation
    response = input("\nProceed with quick bootstrap? (y/n): ").lower()
    if response != 'y':
        print("Bootstrap cancelled.")
        return
    
    # Ask if should clear existing data
    response = input("Clear existing database? (recommended for first run) (y/n): ").lower()
    clear_db = response == 'y'
    
    print("\n🔧 Starting bootstrap process...")
    
    # Create bootstrap instance
    bootstrap = GlobalBootstrap()
    
    # Clear database if requested
    if clear_db:
        print("📦 Initializing clean database...")
        bootstrap.initialize_database()
        if bootstrap.progress_file.exists():
            bootstrap.progress_file.unlink()
    
    # Run quick bootstrap (last 2 years)
    current_year = datetime.now().year
    
    print(f"\n📅 Processing fiscal years {current_year - 1} to {current_year + 1}")
    print("🌍 Processing all regions and sub-regions...")
    print("\nThis will process:")
    print("  • AFRICA (5 sub-regions)")
    print("  • AMERICAS (4 sub-regions)")
    print("  • ASIA (4 sub-regions)")
    print("  • MIDDLE_EAST (2 sub-regions)")
    print("  • EUROPE (4 sub-regions)")
    print("\n" + "-"*60)
    
    # Run the bootstrap
    bootstrap.run_by_subregion(
        start_year=current_year - 1,
        end_year=current_year + 1,
        year_increment=3
    )
    
    print("\n" + "="*60)
    print("✅ QUICK BOOTSTRAP COMPLETE!")
    print("="*60)
    print("\n📊 Summary:")
    print(f"  Total opportunities found: {bootstrap.stats['total_found']:,}")
    print(f"  Total inserted: {bootstrap.stats['total_inserted']:,}")
    
    if bootstrap.stats['by_region']:
        print("\n📍 By Region:")
        for region, count in bootstrap.stats['by_region'].items():
            print(f"    {region}: {count:,}")
    
    print("\n🎉 Your database is ready!")
    print("\nNext steps:")
    print("1. Test the data: python test_data_loading.py")
    print("2. Run the dashboard: streamlit run global_streamlit_dashboard.py")
    print("\n" + "="*60)


if __name__ == "__main__":
    main()