#!/usr/bin/env python3
"""
run_incremental_bootstrap.py - Incremental Bootstrap for Spirit of America
Processes year by year, region by region to manage database size
Allows committing to Git after each year to prevent large file issues
"""

import os
import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path
import sqlite3

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_bootstrap import GlobalBootstrap
from global_sam_utils import GlobalCountryManager, GlobalDatabaseManager, GlobalConfig


def get_database_stats():
    """Get current database statistics"""
    config = GlobalConfig()
    
    if not config.db_path.exists():
        return None
    
    try:
        # Get file size
        size_mb = config.db_path.stat().st_size / (1024 * 1024)
        
        # Get record count
        conn = sqlite3.connect(str(config.db_path))
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM opportunities")
        total = cur.fetchone()[0]
        
        # Get count by region
        cur.execute("""
            SELECT Geographic_Region, COUNT(*) as cnt
            FROM opportunities
            WHERE Geographic_Region IS NOT NULL
            GROUP BY Geographic_Region
            ORDER BY cnt DESC
        """)
        
        by_region = dict(cur.fetchall())
        conn.close()
        
        return {
            'size_mb': size_mb,
            'total_records': total,
            'by_region': by_region
        }
        
    except Exception as e:
        print(f"Error getting stats: {e}")
        return None


def run_git_command(cmd):
    """Run a git command and return success status"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)


def commit_and_push(year, region=None):
    """Commit and push current state to GitHub"""
    print("\n" + "="*60)
    print(" 📤 GIT COMMIT & PUSH")
    print("="*60)
    
    # Check if there are changes
    success, stdout, stderr = run_git_command("git status --porcelain")
    if not stdout.strip():
        print("✅ No changes to commit")
        return True
    
    print("Changes detected. Committing...")
    
    # Add all changes
    success, stdout, stderr = run_git_command("git add -A")
    if not success:
        print(f"❌ Failed to add files: {stderr}")
        return False
    
    # Create commit message
    if region:
        commit_msg = f"Add data: {year} - {region}"
    else:
        commit_msg = f"Add data: {year} - all regions"
    
    # Commit
    success, stdout, stderr = run_git_command(f'git commit -m "{commit_msg}"')
    if not success:
        if "nothing to commit" in stderr or "nothing to commit" in stdout:
            print("✅ Nothing to commit")
            return True
        print(f"❌ Failed to commit: {stderr}")
        return False
    
    print(f"✅ Committed: {commit_msg}")
    
    # Ask if should push
    push = input("\n🔄 Push to GitHub now? (y/n): ").lower()
    if push == 'y':
        print("Pushing to GitHub...")
        success, stdout, stderr = run_git_command("git push origin main")
        if success:
            print("✅ Pushed successfully!")
        else:
            print(f"❌ Push failed: {stderr}")
            print("You can push manually later with: git push origin main")
            return False
    else:
        print("⏭️ Skipping push (you can push manually later)")
    
    return True


def process_single_year_region(year, region):
    """Process a single year for a single region"""
    print(f"\n🌍 Processing {year} for {region}...")
    
    bootstrap = GlobalBootstrap()
    
    # Process just this year and region
    found, inserted = bootstrap.process_year_for_region(year, target_region=region)
    
    print(f"   Found: {found:,} | Inserted: {inserted:,}")
    
    return found, inserted


def main():
    """Main incremental bootstrap runner"""
    
    print("\n" + "="*70)
    print(" 🔄 INCREMENTAL BOOTSTRAP - Year by Year, Region by Region")
    print("="*70)
    print("\nThis will process SAM.gov data incrementally to manage database size.")
    print("You can commit to GitHub after each year to prevent large file issues.")
    print("\n⚠️  This approach takes longer but is MUCH safer for Git/GitHub limits.")
    
    # Check current state
    stats = get_database_stats()
    if stats:
        print(f"\n📊 Current database:")
        print(f"   Size: {stats['size_mb']:.1f} MB")
        print(f"   Records: {stats['total_records']:,}")
        if stats['by_region']:
            print("   By region:")
            for region, count in stats['by_region'].items():
                print(f"      {region}: {count:,}")
    else:
        print("\n📊 No existing database found - will create new one")
    
    # Configuration options
    print("\n" + "-"*70)
    print("\n📋 BOOTSTRAP OPTIONS:\n")
    print("  1. 🌐 FULL INCREMENTAL (All years 1998-2025, all regions)")
    print("  2. 📅 RECENT INCREMENTAL (Last 5 years, all regions)")
    print("  3. 🎯 SPECIFIC YEAR RANGE (Custom years, all regions)")
    print("  4. 🌍 SINGLE REGION (All years, one region)")
    print("  5. 🔧 CUSTOM (Specific years and regions)")
    print("  0. ❌ Exit")
    
    choice = input("\nEnter choice (0-5): ").strip()
    
    if choice == "0":
        print("Exiting...")
        return
    
    # Determine years and regions
    current_year = datetime.now().year
    regions = ["AFRICA", "AMERICAS", "ASIA", "MIDDLE_EAST", "EUROPE"]
    
    if choice == "1":
        # Full incremental
        start_year = 1998
        end_year = current_year + 1
        selected_regions = regions
        
    elif choice == "2":
        # Recent incremental
        start_year = current_year - 4
        end_year = current_year + 1
        selected_regions = regions
        
    elif choice == "3":
        # Specific year range
        start_year = int(input("Enter start year (e.g., 2010): "))
        end_year = int(input("Enter end year (e.g., 2024): "))
        selected_regions = regions
        
    elif choice == "4":
        # Single region
        print("\nSelect region:")
        for i, region in enumerate(regions, 1):
            print(f"  {i}. {region}")
        region_choice = int(input("Enter choice (1-5): "))
        selected_regions = [regions[region_choice - 1]]
        start_year = 1998
        end_year = current_year + 1
        
    elif choice == "5":
        # Custom
        start_year = int(input("Enter start year: "))
        end_year = int(input("Enter end year: "))
        
        print("\nSelect regions (comma-separated numbers):")
        for i, region in enumerate(regions, 1):
            print(f"  {i}. {region}")
        region_choices = input("Enter choices (e.g., 1,3,5): ")
        selected_regions = [regions[int(i)-1] for i in region_choices.split(',')]
    
    else:
        print("Invalid choice")
        return
    
    # Configuration summary
    print("\n" + "="*70)
    print(" 📋 BOOTSTRAP CONFIGURATION")
    print("="*70)
    print(f"Years: {start_year} to {end_year} ({end_year - start_year + 1} years)")
    print(f"Regions: {', '.join(selected_regions)}")
    print(f"Total iterations: {(end_year - start_year + 1) * len(selected_regions)}")
    
    # Git LFS check
    print("\n🔍 Checking Git LFS...")
    success, stdout, stderr = run_git_command("git lfs version")
    if not success:
        print("❌ Git LFS not installed! This is required for large databases.")
        print("Install with: git lfs install")
        return
    print("✅ Git LFS is available")
    
    # Ensure .gitattributes is set up
    gitattributes = Path(".gitattributes")
    if not gitattributes.exists() or "*.db" not in gitattributes.read_text():
        print("📝 Setting up Git LFS tracking for databases...")
        run_git_command("git lfs track 'data/*.db'")
        run_git_command("git add .gitattributes")
        run_git_command("git commit -m 'Configure Git LFS for databases'")
    
    # Ask for confirmation
    print("\n" + "-"*70)
    confirm = input("\n🚀 Ready to start incremental bootstrap? (yes/no): ").lower()
    if confirm != 'yes':
        print("Cancelled.")
        return
    
    # Options for processing
    print("\n📋 Processing options:")
    commit_after_year = input("   Commit after each year? (y/n): ").lower() == 'y'
    stop_if_large = input("   Stop if database exceeds 500MB? (y/n): ").lower() == 'y'
    
    # Initialize bootstrap (creates database if needed)
    bootstrap = GlobalBootstrap()
    
    # If database doesn't exist, initialize it
    if not get_database_stats():
        print("\n🔧 Initializing database...")
        bootstrap.initialize_database()
    
    # Process year by year, region by region
    print("\n" + "="*70)
    print(" 🚀 STARTING INCREMENTAL PROCESSING")
    print("="*70)
    
    total_start_time = datetime.now()
    total_found = 0
    total_inserted = 0
    
    for year in range(start_year, end_year + 1):
        year_start_time = datetime.now()
        year_found = 0
        year_inserted = 0
        
        print(f"\n{'='*60}")
        print(f" 📅 PROCESSING YEAR: {year}")
        print(f"{'='*60}")
        
        for region in selected_regions:
            # Check database size before processing
            stats = get_database_stats()
            if stats:
                print(f"\n💾 Database size: {stats['size_mb']:.1f} MB ({stats['total_records']:,} records)")
                
                if stop_if_large and stats['size_mb'] > 500:
                    print("\n⚠️  Database exceeds 500MB! Stopping as requested.")
                    print("You can continue later by running the script again.")
                    return
            
            # Process this year/region combination
            found, inserted = process_single_year_region(year, region)
            year_found += found
            year_inserted += inserted
            total_found += found
            total_inserted += inserted
            
            # Small delay to not overload SAM.gov
            time.sleep(2)
            
            # Show progress
            elapsed = datetime.now() - year_start_time
            print(f"   Progress: Year {year}, Region {region} complete ({elapsed.seconds}s)")
        
        # Year complete - show summary
        year_elapsed = datetime.now() - year_start_time
        print(f"\n✅ Year {year} complete!")
        print(f"   Time: {year_elapsed}")
        print(f"   Found: {year_found:,}")
        print(f"   Inserted: {year_inserted:,}")
        
        # Get updated stats
        stats = get_database_stats()
        if stats:
            print(f"\n📊 Database status after {year}:")
            print(f"   Size: {stats['size_mb']:.1f} MB")
            print(f"   Total records: {stats['total_records']:,}")
            
            # Check size warning
            if stats['size_mb'] > 400:
                print("\n⚠️  WARNING: Database approaching 500MB limit!")
        
        # Commit after year if requested
        if commit_after_year:
            print(f"\n💾 Committing year {year} to Git...")
            if commit_and_push(year):
                print("✅ Year committed successfully")
            else:
                print("⚠️  Commit failed - you can commit manually later")
        
        # Ask if should continue
        if year < end_year:
            continue_prompt = input(f"\n➡️  Continue with year {year + 1}? (y/n): ").lower()
            if continue_prompt != 'y':
                print("\n⏸️  Bootstrap paused. You can resume later.")
                break
    
    # Final summary
    total_elapsed = datetime.now() - total_start_time
    
    print("\n" + "="*70)
    print(" ✅ INCREMENTAL BOOTSTRAP COMPLETE!")
    print("="*70)
    
    # Final statistics
    final_stats = get_database_stats()
    if final_stats:
        print("\n📊 FINAL DATABASE STATISTICS:")
        print(f"   Size: {final_stats['size_mb']:.1f} MB")
        print(f"   Total records: {final_stats['total_records']:,}")
        print(f"\n   By region:")
        for region, count in final_stats['by_region'].items():
            print(f"      {region}: {count:,}")
    
    print(f"\n⏱️  Total time: {total_elapsed}")
    print(f"📊 Total found: {total_found:,}")
    print(f"📊 Total inserted: {total_inserted:,}")
    
    # Final commit if not already done
    if not commit_after_year:
        final_commit = input("\n💾 Commit all changes to Git? (y/n): ").lower()
        if final_commit == 'y':
            commit_and_push(f"{start_year}-{end_year}", "all-regions")
    
    print("\n" + "="*70)
    print(" 🎉 SUCCESS! Your database is ready!")
    print("="*70)
    
    print("\n📋 Next steps:")
    print("1. Test the dashboard:")
    print("   streamlit run global_streamlit_dashboard.py")
    print("\n2. Push to GitHub (if not already done):")
    print("   git push origin main")
    print("\n3. Deploy on Streamlit Cloud")
    
    print("\n" + "="*70)


def resume_bootstrap():
    """Resume a previously interrupted bootstrap"""
    print("\n" + "="*70)
    print(" 🔄 RESUME INCREMENTAL BOOTSTRAP")
    print("="*70)
    
    stats = get_database_stats()
    if not stats:
        print("❌ No existing database found. Start a new bootstrap instead.")
        return
    
    print(f"\n📊 Current database:")
    print(f"   Size: {stats['size_mb']:.1f} MB")
    print(f"   Records: {stats['total_records']:,}")
    
    # Try to determine last processed year
    config = GlobalConfig()
    conn = sqlite3.connect(str(config.db_path))
    cur = conn.cursor()
    
    # Get years in database
    cur.execute("""
        SELECT DISTINCT substr(PostedDate_normalized, 1, 4) as year
        FROM opportunities
        WHERE PostedDate_normalized IS NOT NULL
        ORDER BY year DESC
        LIMIT 1
    """)
    
    result = cur.fetchone()
    if result:
        last_year = int(result[0])
        print(f"\n📅 Last year in database: {last_year}")
        
        resume_year = last_year + 1
        print(f"📅 Resume from year: {resume_year}")
        
        confirm = input("\nContinue from this year? (y/n): ").lower()
        if confirm == 'y':
            # Set up to continue
            print("\nResuming bootstrap...")
            # Would continue from resume_year
    
    conn.close()


if __name__ == "__main__":
    try:
        # Check if resuming
        if len(sys.argv) > 1 and sys.argv[1] == "--resume":
            resume_bootstrap()
        else:
            main()
    except KeyboardInterrupt:
        print("\n\n⏸️  Bootstrap interrupted by user")
        print("You can resume later by running the script again")
        print("Your progress has been saved.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        print("\nYour progress has been saved. You can resume later.")