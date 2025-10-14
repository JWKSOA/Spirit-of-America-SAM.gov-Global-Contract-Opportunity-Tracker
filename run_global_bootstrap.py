#!/usr/bin/env python3
"""
run_global_bootstrap.py - Easy runner for Spirit of America global bootstrap
Simple menu-driven script to get your dashboard running quickly
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_bootstrap import GlobalBootstrap
from global_sam_utils import GlobalCountryManager, GlobalDatabaseManager, GlobalConfig


def test_database():
    """Quick test to see what's in the database"""
    config = GlobalConfig()
    
    if not config.db_path.exists():
        print("\n❌ No database found yet")
        return False
    
    try:
        import sqlite3
        conn = sqlite3.connect(str(config.db_path))
        cur = conn.cursor()
        
        # Get counts
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total = cur.fetchone()[0]
        
        print(f"\n📊 Current Database Status:")
        print(f"   Total records: {total:,}")
        
        if total > 0:
            # By region
            cur.execute("""
                SELECT Geographic_Region, COUNT(*) as cnt
                FROM opportunities
                WHERE Geographic_Region IS NOT NULL
                GROUP BY Geographic_Region
                ORDER BY cnt DESC
            """)
            
            print("\n   Records by region:")
            for region, count in cur.fetchall():
                print(f"     {region}: {count:,}")
        
        conn.close()
        return total > 0
        
    except Exception as e:
        print(f"\n❌ Error checking database: {e}")
        return False


def main():
    """Main runner with simple menu"""
    
    print("\n" + "="*70)
    print(" 🌐 Spirit of America: Global SAM.gov Bootstrap Runner")
    print("="*70)
    print("\nThis tool will help you bootstrap SAM.gov contract data")
    print("for all countries worldwide, organized by Spirit of America regions.")
    
    # Check current status
    has_data = test_database()
    
    print("\n" + "-"*70)
    print("\nChoose an option:\n")
    print("  1. 🚀 QUICK START (Last 2 years, all regions) - Recommended")
    print("  2. 📅 LAST 5 YEARS (All regions)")
    print("  3. 📚 FULL HISTORY (1998-present, all regions) - Takes hours")
    print("  4. 🌍 SPECIFIC REGION (Choose region and years)")
    print("  5. 🔄 DAILY UPDATE (Update recent data only)")
    print("  6. 🧹 CLEAR AND START FRESH")
    print("  0. ❌ Exit")
    print("\n" + "-"*70)
    
    choice = input("\nEnter your choice (0-6): ").strip()
    
    if choice == "0":
        print("\nExiting...")
        return
    
    # Create bootstrap instance
    bootstrap = GlobalBootstrap()
    current_year = datetime.now().year
    
    if choice == "1":
        # Quick start - last 2 years
        print("\n🚀 QUICK START: Processing last 2 years for all regions...")
        print("Estimated time: 10-20 minutes\n")
        
        confirm = input("Continue? (y/n): ").lower()
        if confirm != 'y':
            print("Cancelled.")
            return
        
        # Clear if no data
        clear = not has_data
        if has_data:
            clear_prompt = input("Clear existing data? (y/n): ").lower()
            clear = clear_prompt == 'y'
        
        bootstrap.run_bootstrap(
            start_year=current_year - 1,
            end_year=current_year + 1,
            clear=clear
        )
        
    elif choice == "2":
        # Last 5 years
        print("\n📅 Processing last 5 years for all regions...")
        print("Estimated time: 30-45 minutes\n")
        
        confirm = input("Continue? (y/n): ").lower()
        if confirm != 'y':
            print("Cancelled.")
            return
        
        clear = False
        if has_data:
            clear_prompt = input("Clear existing data? (y/n): ").lower()
            clear = clear_prompt == 'y'
        
        bootstrap.run_bootstrap(
            start_year=current_year - 4,
            end_year=current_year + 1,
            clear=clear
        )
        
    elif choice == "3":
        # Full history
        print("\n⚠️ WARNING: Full historical bootstrap will take 2-3 hours!")
        print("Processing FY1998 to present for all regions...\n")
        
        confirm = input("Are you sure? (yes/no): ").lower()
        if confirm != 'yes':
            print("Cancelled.")
            return
        
        clear = input("Clear existing data? (recommended) (y/n): ").lower() == 'y'
        
        bootstrap.run_bootstrap(
            start_year=1998,
            end_year=current_year + 1,
            clear=clear
        )
        
    elif choice == "4":
        # Specific region
        print("\nSelect a region:")
        print("  1. AFRICA")
        print("  2. AMERICAS")
        print("  3. ASIA")
        print("  4. MIDDLE_EAST")
        print("  5. EUROPE")
        
        region_choice = input("\nEnter region number (1-5): ").strip()
        
        regions = {
            "1": "AFRICA",
            "2": "AMERICAS",
            "3": "ASIA",
            "4": "MIDDLE_EAST",
            "5": "EUROPE"
        }
        
        if region_choice not in regions:
            print("Invalid choice.")
            return
        
        target_region = regions[region_choice]
        
        print(f"\nSelected region: {target_region}")
        print("\nSelect time period:")
        print("  1. Last 2 years")
        print("  2. Last 5 years")
        print("  3. Last 10 years")
        print("  4. Custom range")
        
        period_choice = input("\nEnter choice (1-4): ").strip()
        
        if period_choice == "1":
            start_year = current_year - 1
            end_year = current_year + 1
        elif period_choice == "2":
            start_year = current_year - 4
            end_year = current_year + 1
        elif period_choice == "3":
            start_year = current_year - 9
            end_year = current_year + 1
        elif period_choice == "4":
            start_year = int(input("Enter start year (e.g., 2015): "))
            end_year = int(input("Enter end year (e.g., 2024): "))
        else:
            print("Invalid choice.")
            return
        
        print(f"\nProcessing {target_region} from FY{start_year} to FY{end_year}...")
        
        confirm = input("Continue? (y/n): ").lower()
        if confirm != 'y':
            print("Cancelled.")
            return
        
        clear = False
        if has_data:
            clear_prompt = input("Clear existing data? (y/n): ").lower()
            clear = clear_prompt == 'y'
        
        bootstrap.run_bootstrap(
            start_year=start_year,
            end_year=end_year,
            target_region=target_region,
            clear=clear
        )
        
    elif choice == "5":
        # Daily update
        print("\n🔄 Running daily update (last 14 days)...")
        
        if not has_data:
            print("❌ No database found! Run bootstrap first.")
            return
        
        from global_daily_update import GlobalDailyUpdater
        
        updater = GlobalDailyUpdater(lookback_days=14)
        success = updater.run()
        
        if success:
            print("\n✅ Daily update completed!")
        else:
            print("\n❌ Daily update failed.")
        return
        
    elif choice == "6":
        # Clear and start fresh
        print("\n🧹 Clearing database and starting fresh...")
        
        confirm = input("This will delete all existing data. Continue? (yes/no): ").lower()
        if confirm != 'yes':
            print("Cancelled.")
            return
        
        bootstrap.initialize_database()
        
        # Clear progress file
        progress_file = bootstrap.config.data_dir / "bootstrap_progress.json"
        if progress_file.exists():
            progress_file.unlink()
        
        print("\n✅ Database cleared!")
        print("Now run option 1 or 2 to load data.")
        return
        
    else:
        print("\nInvalid choice.")
        return
    
    # After bootstrap completes
    print("\n" + "="*70)
    print(" ✅ Bootstrap Complete!")
    print("="*70)
    
    # Show final stats
    test_database()
    
    print("\n🎉 Your database is ready!")
    print("\nNext steps:")
    print("1. Test the dashboard:")
    print("   streamlit run global_streamlit_dashboard.py")
    print("\n2. Set up daily updates:")
    print("   python global_daily_update.py")
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