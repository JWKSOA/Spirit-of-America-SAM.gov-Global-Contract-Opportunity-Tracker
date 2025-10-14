#!/usr/bin/env python3
"""
sanity_check_git_lfs.py - Pre-Bootstrap Git LFS Verification
Run this BEFORE starting a full history bootstrap to ensure everything will work
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

def run_command(cmd, check=True):
    """Run a shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=check)
        return result.stdout.strip(), result.stderr.strip(), result.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout, e.stderr, e.returncode

def check_git_lfs():
    """Comprehensive Git LFS sanity check"""
    print("\n" + "="*70)
    print(" 🔍 GIT LFS SANITY CHECK FOR LARGE DATABASE")
    print("="*70)
    print("\nThis will verify your Git LFS setup BEFORE you run the full bootstrap")
    print("to ensure your large database file will be handled properly.\n")
    
    all_good = True
    
    # 1. Check Git LFS is installed
    print("1️⃣  Checking Git LFS installation...")
    stdout, stderr, code = run_command("git lfs version", check=False)
    if code == 0:
        print(f"   ✅ Git LFS installed: {stdout}")
    else:
        print("   ❌ Git LFS not installed!")
        print("   Run: git lfs install")
        all_good = False
        return False
    
    # 2. Check Git LFS is initialized in repo
    print("\n2️⃣  Checking Git LFS initialization...")
    stdout, stderr, code = run_command("git lfs env", check=False)
    if "git config filter.lfs.process" in stdout:
        print("   ✅ Git LFS is initialized in this repository")
    else:
        print("   ⚠️  Git LFS may not be initialized")
        print("   Run: git lfs install")
        all_good = False
    
    # 3. Check .gitattributes file
    print("\n3️⃣  Checking .gitattributes configuration...")
    gitattributes = Path(".gitattributes")
    if gitattributes.exists():
        content = gitattributes.read_text()
        print(f"   ✅ .gitattributes exists")
        
        # Check for database tracking
        if "*.db" in content or "data/*.db" in content:
            print("   ✅ Database files are configured for LFS")
            print("\n   Current LFS tracking rules:")
            for line in content.splitlines():
                if line.strip() and not line.startswith("#"):
                    print(f"      {line}")
        else:
            print("   ⚠️  Database files not tracked in .gitattributes")
            print("   Adding LFS tracking for database files...")
            with open(".gitattributes", "a") as f:
                f.write("\n# Track database files with Git LFS\n")
                f.write("*.db filter=lfs diff=lfs merge=lfs -text\n")
                f.write("data/*.db filter=lfs diff=lfs merge=lfs -text\n")
            print("   ✅ Added database tracking to .gitattributes")
    else:
        print("   ⚠️  No .gitattributes file found")
        print("   Creating .gitattributes with LFS rules...")
        with open(".gitattributes", "w") as f:
            f.write("# Git LFS tracking rules\n")
            f.write("*.db filter=lfs diff=lfs merge=lfs -text\n")
            f.write("data/*.db filter=lfs diff=lfs merge=lfs -text\n")
        print("   ✅ Created .gitattributes with database tracking")
    
    # 4. Check current database file status
    print("\n4️⃣  Checking current database file...")
    db_path = Path("data/global_opportunities.db")
    if db_path.exists():
        size_mb = db_path.stat().st_size / (1024 * 1024)
        print(f"   📊 Current database size: {size_mb:.1f} MB")
        
        if size_mb > 100:
            print("   ⚠️  Database is over 100MB - Git LFS is required!")
        elif size_mb > 50:
            print("   ⚠️  Database is over 50MB - Git LFS is recommended")
        else:
            print("   ✅ Database is under 50MB - Git LFS optional but recommended")
        
        # Check if already tracked by LFS
        stdout, stderr, code = run_command(f"git check-attr filter {db_path}", check=False)
        if "lfs" in stdout:
            print("   ✅ Database is configured for LFS tracking")
        else:
            print("   ⚠️  Database not yet tracked by LFS")
    else:
        print("   📝 No database file exists yet (will be created by bootstrap)")
    
    # 5. Check GitHub LFS quota
    print("\n5️⃣  GitHub LFS Storage Information...")
    print("   📊 GitHub LFS Free Tier Limits:")
    print("      • Storage: 1 GB per month")
    print("      • Bandwidth: 1 GB per month")
    print("\n   📝 Expected database sizes:")
    print("      • 2 years of data: ~50-100 MB")
    print("      • 5 years of data: ~150-300 MB")
    print("      • Full history (1998-2025): ~500-800 MB")
    
    if db_path.exists() and size_mb > 500:
        print("\n   ⚠️  WARNING: Full history may exceed GitHub free tier!")
        print("      Consider using 5-year history instead")
    
    # 6. Test LFS with a dummy file
    print("\n6️⃣  Testing Git LFS with dummy file...")
    test_file = Path("data/.lfs_test.db")
    try:
        # Create small test file
        test_file.write_text("LFS Test")
        
        # Track with LFS
        run_command("git lfs track 'data/.lfs_test.db'", check=False)
        
        # Add to git
        stdout, stderr, code = run_command(f"git add {test_file}", check=False)
        if code == 0:
            print("   ✅ Git LFS can track files successfully")
        else:
            print(f"   ⚠️  Issue with Git LFS: {stderr}")
        
        # Clean up
        run_command(f"git rm --cached {test_file}", check=False)
        test_file.unlink(missing_ok=True)
        
    except Exception as e:
        print(f"   ⚠️  Could not complete LFS test: {e}")
    
    # 7. Check remote repository
    print("\n7️⃣  Checking remote repository...")
    stdout, stderr, code = run_command("git remote -v", check=False)
    if "github.com" in stdout:
        print("   ✅ Connected to GitHub repository")
        # Extract repo name
        for line in stdout.splitlines():
            if "origin" in line and "push" in line:
                print(f"      {line}")
    else:
        print("   ❌ No GitHub remote found!")
        all_good = False
    
    # 8. Check for existing LFS objects
    print("\n8️⃣  Checking existing LFS objects...")
    stdout, stderr, code = run_command("git lfs ls-files", check=False)
    if stdout:
        print("   📦 Current LFS tracked files:")
        for line in stdout.splitlines()[:5]:  # Show first 5
            print(f"      {line}")
    else:
        print("   📝 No files currently tracked by LFS")
    
    # Final recommendations
    print("\n" + "="*70)
    if all_good:
        print(" ✅ GIT LFS SANITY CHECK PASSED!")
    else:
        print(" ⚠️  SOME ISSUES FOUND - See above for details")
    print("="*70)
    
    print("\n📋 RECOMMENDATIONS FOR FULL HISTORY BOOTSTRAP:\n")
    
    print("1. DATABASE SIZE ESTIMATES:")
    print("   • Full history will create a ~500-800 MB database")
    print("   • This WILL require Git LFS (over GitHub's 100MB limit)")
    print("   • May exceed GitHub's free LFS quota (1GB)")
    
    print("\n2. RECOMMENDED APPROACH:")
    print("   Option A: Use 5-year history instead (smaller, ~200-300 MB)")
    print("   Option B: Use full history but be prepared for:")
    print("             - Longer upload times")
    print("             - Possible LFS quota warnings")
    print("             - May need to purchase LFS data pack ($5/month for 50GB)")
    
    print("\n3. BEFORE RUNNING FULL BOOTSTRAP:")
    print("   ✓ Make sure .gitattributes is committed:")
    print("     git add .gitattributes")
    print("     git commit -m 'Configure Git LFS for large database'")
    print("     git push")
    
    print("\n4. AFTER BOOTSTRAP COMPLETES:")
    print("   The large database will be automatically handled by LFS")
    print("   When you git add and push, it will upload to LFS storage")
    
    print("\n5. ALTERNATIVE FOR VERY LARGE DATABASE:")
    print("   Consider hosting database separately (AWS S3, Google Drive)")
    print("   And downloading it during Streamlit deployment")
    
    return all_good


def estimate_bootstrap_size():
    """Estimate database size for different time periods"""
    print("\n" + "="*70)
    print(" 📊 DATABASE SIZE ESTIMATOR")
    print("="*70)
    
    estimates = {
        "Last 2 years (Quick Start)": "50-100 MB",
        "Last 5 years": "150-300 MB", 
        "Last 10 years": "300-500 MB",
        "Full history (1998-2025)": "500-800 MB",
    }
    
    print("\nEstimated database sizes for different periods:\n")
    for period, size in estimates.items():
        print(f"  • {period:30} → {size}")
        
    print("\n📝 Git/GitHub limits:")
    print("  • GitHub file limit (without LFS): 100 MB")
    print("  • GitHub repo recommended max: 1 GB")
    print("  • Git LFS free tier: 1 GB storage + 1 GB bandwidth/month")
    
    print("\n💡 Recommendations:")
    print("  • Under 100 MB: Can use regular Git (but LFS recommended)")
    print("  • 100-500 MB: Must use Git LFS")
    print("  • Over 500 MB: Consider upgrading LFS or using 5-year history")


def main():
    """Run all checks"""
    try:
        # Run Git LFS check
        lfs_ok = check_git_lfs()
        
        # Show size estimates
        estimate_bootstrap_size()
        
        # Final decision helper
        print("\n" + "="*70)
        print(" 🤔 SHOULD YOU RUN FULL HISTORY BOOTSTRAP?")
        print("="*70)
        
        if lfs_ok:
            print("\n✅ Git LFS is properly configured")
            print("\n📋 You can proceed with bootstrap, but consider:")
            print("   1. Start with 5-year history first (safer)")
            print("   2. Monitor the database size as it builds")
            print("   3. Be prepared for long upload times (500MB+ file)")
            print("   4. Watch for GitHub LFS quota warnings")
            
            print("\n💡 RECOMMENDED APPROACH:")
            print("   1. Run 5-year bootstrap first")
            print("   2. Test push to GitHub")
            print("   3. If successful, consider full history later")
        else:
            print("\n⚠️  Fix Git LFS issues before running full bootstrap!")
            print("\nRequired fixes:")
            print("   1. Install Git LFS: git lfs install")
            print("   2. Commit .gitattributes file")
            print("   3. Run this check again")
        
        print("\n" + "="*70)
        print(f" Sanity check completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70 + "\n")
        
        return lfs_ok
        
    except Exception as e:
        print(f"\n❌ Error during sanity check: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)