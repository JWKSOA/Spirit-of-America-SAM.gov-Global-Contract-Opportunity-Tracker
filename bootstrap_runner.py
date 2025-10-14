#!/usr/bin/env python3
"""
bootstrap_runner.py - Easy menu-based bootstrap runner for processing by sub-region
Makes it simple to bootstrap data one sub-region at a time
"""

import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_bootstrap import GlobalBootstrap

def print_menu():
    """Print menu options"""
    print("\n" + "="*60)
    print("SAM.gov Global Bootstrap - Sub-Region Runner")
    print("="*60)
    print("\nSelect what to bootstrap:")
    print("\n1. QUICK START (Last 2 years, all regions)")
    print("\n--- BY REGION (All Sub-Regions) ---")
    print("2. AFRICA (All 5 sub-regions)")
    print("3. AMERICAS (All 4 sub-regions)") 
    print("4. ASIA (All 4 sub-regions)")
    print("5. MIDDLE_EAST (All 2 sub-regions)")
    print("6. EUROPE (All 4 sub-regions)")
    print("\n--- BY INDIVIDUAL SUB-REGION ---")
    print("7. Select specific sub-region")
    print("\n--- FULL HISTORY ---")
    print("8. FULL HISTORICAL (1998-present, all regions)")
    print("9. Custom date range")
    print("\n0. Exit")
    print("="*60)

def select_subregion():
    """Interactive sub-region selector"""
    from global_sam_utils import GlobalCountryManager
    
    cm = GlobalCountryManager()
    
    print("\n--- SELECT REGION FIRST ---")
    regions = list(cm.GEOGRAPHIC_REGIONS.keys())
    for i, region in enumerate(regions, 1):
        print(f"{i}. {region}")
    
    try:
        region_idx = int(input("\nSelect region number: ")) - 1
        if 0 <= region_idx < len(regions):
            selected_region = regions[region_idx]
            
            print(f"\n--- SUB-REGIONS IN {selected_region} ---")
            subregions = list(cm.GEOGRAPHIC_REGIONS[selected_region].keys())
            for i, subregion in enumerate(subregions, 1):
                # Count countries in this sub-region
                countries = cm.GEOGRAPHIC_REGIONS[selected_region][subregion]
                print(f"{i}. {subregion} ({len(countries)} countries)")
            
            sub_idx = int(input("\nSelect sub-region number: ")) - 1
            if 0 <= sub_idx < len(subregions):
                return selected_region, subregions[sub_idx]
    except:
        pass
    
    return None, None

def main():
    """Main runner"""
    while True:
        print_menu()
        
        try:
            choice = input("\nEnter your choice (0-9): ").strip()
            
            if choice == "0":
                print("\nExiting...")
                break
            
            bootstrap = GlobalBootstrap()
            current_year = datetime.now().year
            
            if choice == "1":
                # Quick start - last 2 years
                print("\n🚀 QUICK START: Processing last 2 years for all regions...")
                print("This will process each sub-region individually")
                
                # Clear database first
                confirm = input("Clear existing database? (yes/no): ").lower()
                if confirm == "yes":
                    bootstrap.initialize_database()
                    if bootstrap.progress_file.exists():
                        bootstrap.progress_file.unlink()
                
                bootstrap.run_by_subregion(
                    start_year=current_year - 1,
                    end_year=current_year + 1,
                    year_increment=3
                )
                
            elif choice in ["2", "3", "4", "5", "6"]:
                # Process specific region
                regions = {
                    "2": "AFRICA",
                    "3": "AMERICAS",
                    "4": "ASIA",
                    "5": "MIDDLE_EAST",
                    "6": "EUROPE"
                }
                
                region = regions[choice]
                print(f"\n🌍 Processing {region}...")
                
                # Ask for year range
                print("\nYear range options:")
                print("1. Last 2 years (quick)")
                print("2. Last 5 years (recommended)")
                print("3. Last 10 years")
                print("4. Full history (1998-present)")
                
                year_choice = input("Select year range (1-4): ").strip()
                
                year_ranges = {
                    "1": (current_year - 1, current_year + 1),
                    "2": (current_year - 4, current_year + 1),
                    "3": (current_year - 9, current_year + 1),
                    "4": (1998, current_year + 1)
                }
                
                start_year, end_year = year_ranges.get(year_choice, (current_year - 4, current_year + 1))
                
                print(f"\nProcessing {region} from FY{start_year} to FY{end_year}")
                print("This will process each sub-region individually...")
                
                bootstrap.run_by_subregion(
                    target_region=region,
                    start_year=start_year,
                    end_year=end_year,
                    year_increment=5
                )
                
            elif choice == "7":
                # Specific sub-region
                region, subregion = select_subregion()
                
                if region and subregion:
                    print(f"\n📍 Processing {region} - {subregion}...")
                    
                    # Ask for years
                    start_year = int(input("Start year (e.g., 2020): "))
                    end_year = int(input("End year (e.g., 2024): "))
                    
                    bootstrap.run_by_subregion(
                        target_region=region,
                        target_subregion=subregion,
                        start_year=start_year,
                        end_year=end_year,
                        year_increment=5
                    )
                else:
                    print("Invalid selection")
                
            elif choice == "8":
                # Full historical
                print("\n⚠️ WARNING: Full historical bootstrap will take several hours!")
                print("It will process each sub-region individually for all years")
                confirm = input("Are you sure? (yes/no): ").lower()
                
                if confirm == "yes":
                    print("\n📚 Starting FULL HISTORICAL bootstrap (1998-present)...")
                    
                    # Clear database first
                    clear = input("Clear existing database first? (yes/no): ").lower()
                    if clear == "yes":
                        bootstrap.initialize_database()
                        if bootstrap.progress_file.exists():
                            bootstrap.progress_file.unlink()
                    
                    bootstrap.run_by_subregion(
                        start_year=1998,
                        end_year=current_year + 1,
                        year_increment=5
                    )
                
            elif choice == "9":
                # Custom range
                print("\n--- CUSTOM DATE RANGE ---")
                start_year = int(input("Start fiscal year (e.g., 2015): "))
                end_year = int(input("End fiscal year (e.g., 2024): "))
                increment = int(input("Year increment (e.g., 5): "))
                
                print("\nProcess which regions?")
                print("1. All regions")
                print("2. Specific region")
                print("3. Specific sub-region")
                
                scope = input("Choice (1-3): ").strip()
                
                if scope == "2":
                    regions = ["AFRICA", "AMERICAS", "ASIA", "MIDDLE_EAST", "EUROPE"]
                    for i, r in enumerate(regions, 1):
                        print(f"{i}. {r}")
                    
                    reg_idx = int(input("Select region: ")) - 1
                    if 0 <= reg_idx < len(regions):
                        bootstrap.run_by_subregion(
                            target_region=regions[reg_idx],
                            start_year=start_year,
                            end_year=end_year,
                            year_increment=increment
                        )
                elif scope == "3":
                    region, subregion = select_subregion()
                    if region and subregion:
                        bootstrap.run_by_subregion(
                            target_region=region,
                            target_subregion=subregion,
                            start_year=start_year,
                            end_year=end_year,
                            year_increment=increment
                        )
                else:
                    bootstrap.run_by_subregion(
                        start_year=start_year,
                        end_year=end_year,
                        year_increment=increment
                    )
            
            else:
                print("\nInvalid choice. Please try again.")
                continue
            
            print("\n✅ Bootstrap completed!")
            
            # Ask if want to continue
            cont = input("\nRun another bootstrap? (yes/no): ").lower()
            if cont != "yes":
                break
                
        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
            break
        except Exception as e:
            print(f"\nError: {e}")
            continue
    
    print("\nGoodbye!")


if __name__ == "__main__":
    main()