#!/usr/bin/env python3
"""
simple_bootstrap.py - Simple bootstrap for testing
"""

import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from global_bootstrap import GlobalHistoricalBootstrap

def main():
    print("="*60)
    print("Starting Simple Bootstrap - Last 3 Years Only")
    print("="*60)
    
    bootstrap = GlobalHistoricalBootstrap()
    
    # Just load last 3 years for quick testing
    current_year = datetime.now().year
    start_year = current_year - 2
    
    print(f"Loading data from {start_year} to {current_year}")
    
    bootstrap.run(
        start_year=start_year,
        end_year=current_year,
        clear_first=True,
        skip_current=False
    )
    
    print("\n✅ Bootstrap completed!")

if __name__ == "__main__":
    main()