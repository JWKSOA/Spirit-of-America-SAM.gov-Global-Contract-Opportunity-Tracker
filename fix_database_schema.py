#!/usr/bin/env python3
"""
fix_database_schema.py - Migration script to fix database schema
This script will rebuild your database with the complete SAM.gov schema
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    print("\n" + "="*70)
    print(" 🔧 DATABASE SCHEMA FIX FOR SPIRIT OF AMERICA GLOBAL DASHBOARD")
    print("="*70)
    print("\nThis script will fix the missing columns error by rebuilding")
    print("your database with the complete SAM.gov schema.")
    print("\n⚠️  WARNING: This will DELETE your existing database and rebuild it.")
    print("Make sure you have time to re-run the bootstrap after this.")
    
    # Ask for confirmation
    print("\n" + "-"*70)
    confirm = input("\n❓ Do you want to continue? (type 'yes' to proceed): ").strip().lower()
    
    if confirm != 'yes':
        print("\n❌ Operation cancelled. No changes made.")
        return False
    
    print("\n📋 Starting migration process...")
    
    # Step 1: Remove old database
    db_path = Path("data/global_opportunities.db")
    if db_path.exists():
        print(f"\n1️⃣ Removing old database: {db_path}")
        try:
            os.remove(db_path)
            print("   ✅ Old database removed successfully")
        except Exception as e:
            print(f"   ❌ Error removing database: {e}")
            return False
    else:
        print("\n1️⃣ No existing database found - skipping removal")
    
    # Step 2: Remove progress file
    progress_file = Path("data/bootstrap_progress.json")
    if progress_file.exists():
        print(f"\n2️⃣ Removing progress file: {progress_file}")
        try:
            os.remove(progress_file)
            print("   ✅ Progress file removed successfully")
        except Exception as e:
            print(f"   ❌ Error removing progress file: {e}")
    else:
        print("\n2️⃣ No progress file found - skipping")
    
    # Step 3: Create new database with correct schema
    print("\n3️⃣ Creating new database with complete SAM.gov schema...")
    
    try:
        # Import the fixed global_sam_utils
        from global_sam_utils import GlobalConfig, GlobalCountryManager, GlobalDatabaseManager
        
        # Initialize components
        config = GlobalConfig()
        country_manager = GlobalCountryManager()
        db_manager = GlobalDatabaseManager(config, country_manager)
        
        # Create new database with complete schema
        db_manager.initialize_database()
        print("   ✅ New database created with all required columns")
        
        # Verify the schema
        print("\n4️⃣ Verifying new database schema...")
        import sqlite3
        conn = sqlite3.connect(str(config.db_path))
        cur = conn.cursor()
        
        # Check for critical columns that were missing
        cur.execute("PRAGMA table_info(opportunities)")
        columns = [col[1] for col in cur.fetchall()]
        
        critical_columns = ['Office', 'Sub-Tier', 'SetASide', 'NaicsCode', 
                          'CGAC', 'FPDS Code', 'AAC Code']
        
        missing = []
        for col in critical_columns:
            if col not in columns:
                missing.append(col)
        
        if missing:
            print(f"   ⚠️  Warning: Still missing columns: {missing}")
            print("      Make sure global_sam_utils.py has been updated!")
        else:
            print("   ✅ All critical columns verified!")
        
        # Show all columns
        print(f"\n   📊 Total columns in new schema: {len(columns)}")
        print("   Key columns present:")
        for col in ['NoticeId', 'Title', 'Office', 'Department/Ind.Agency', 
                   'Sub-Tier', 'PopCountry', 'Geographic_Region', 'Link']:
            if col in columns:
                print(f"      ✅ {col}")
            else:
                print(f"      ❌ {col} (MISSING!)")
        
        conn.close()
        
    except ImportError as e:
        print(f"\n❌ Error: Could not import global_sam_utils.py")
        print(f"   Details: {e}")
        print("\n   Make sure you have updated global_sam_utils.py first!")
        return False
    except Exception as e:
        print(f"\n❌ Error creating database: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Success!
    print("\n" + "="*70)
    print(" ✅ DATABASE SCHEMA FIX COMPLETE!")
    print("="*70)
    
    print("\n📋 Next Steps:")
    print("\n1. Run the bootstrap to reload data:")
    print("   python run_global_bootstrap.py")
    print("\n2. Choose Option 1 (Quick Start) to test with 2 years of data")
    print("\n3. Once data loads, test the dashboard:")
    print("   streamlit run global_streamlit_dashboard.py")
    print("\n4. If everything works, commit and push to GitHub:")
    print("   git add -A")
    print("   git commit -m 'Fix: Complete SAM.gov schema - all columns added'")
    print("   git push origin main")
    
    print("\n" + "="*70)
    print(f" Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)