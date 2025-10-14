#!/usr/bin/env python3
"""
simple_bootstrap.py - Simple bootstrap without Git operations for testing
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Fix the import to use the correct filename
from global_bootstrap import GlobalHistoricalBootstrap

def main():
    print("="*60)
    print("Starting Simple Bootstrap")
    print("="*60)
    
    # Create bootstrap instance
    bootstrap = GlobalHistoricalBootstrap()
    
    # Run quick start (last 5 years)
    current_year = datetime.now().year
    start_year = current_year - 4
    
    print(f"Loading data from {start_year} to {current_year}")
    
    bootstrap.run(
        start_year=start_year,
        end_year=current_year,
        clear_first=True,  # Start fresh
        skip_current=False
    )
    
    print("\n✅ Bootstrap completed!")

if __name__ == "__main__":
    main()