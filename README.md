# 🌐 Spirit of America: SAM.gov Global Contract Opportunity Tracker

[![Daily Update](https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker/actions/workflows/update-global-sam-db.yml/badge.svg)](https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker/actions/workflows/update-global-sam-db.yml)

An automated dashboard for tracking U.S. government contract opportunities worldwide, organized by Spirit of America's five geographic portfolios.

## ✅ Current Status

**Last Updated**: December 2024  
**Data Coverage**: FY2023-2025  
**Total Opportunities**: Active database with global coverage  
**Regions**: All 5 Spirit of America portfolios operational

## 🌍 Overview

This system automatically collects, processes, and visualizes contract opportunities from SAM.gov for 195+ countries, organized into Spirit of America's geographic portfolios:

- **AFRICA** - 54 countries across 5 sub-regions
- **AMERICAS** - 35+ countries across 4 sub-regions  
- **ASIA** - 50+ countries across 4 sub-regions (including Oceania)
- **MIDDLE_EAST** - 20 countries across 2 sub-regions
- **EUROPE** - 45+ countries across 4 sub-regions

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- 2GB RAM minimum
- 500MB disk space

### Installation & Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker.git
cd Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker

# Install dependencies
pip install -r requirements.txt

# Run the easy setup script
python run_global_bootstrap.py
# Choose option 1 for Quick Start (last 2 years of data)

# Test the data loaded correctly
python test_global_data.py

# Launch the dashboard
streamlit run global_streamlit_dashboard.py
```

## ✨ Key Features

### Data Collection & Processing
- **Automated Daily Updates** via GitHub Actions
- **Smart Country Recognition** - Handles 195+ countries with variations
- **Regional Classification** - Automatic assignment to Spirit of America portfolios
- **Historical Archives** - Access to data from FY1998 to present
- **Incremental Updates** - Only processes new/changed data

### Dashboard Features
- **Geographic Heat Maps** - Visual representation by region and country
- **Sub-regional Analysis** - Detailed breakdown by portfolio
- **Interactive Tables** - Searchable, sortable with direct SAM.gov links
- **Temporal Trends** - Track opportunities over time
- **Export Capabilities** - Download filtered data as CSV

### Technical Features
- **Fixed CSV Encoding** - Robust handling of all SAM.gov data formats
- **Memory Optimization** - Chunked processing for large files
- **Progress Tracking** - Resume capability if interrupted
- **Error Recovery** - Automatic retries and S3 fallbacks
- **Database Indexing** - Fast queries on large datasets

## 📁 Project Structure

```
├── global_sam_utils.py          # Core utilities with fixed CSV reader
├── global_bootstrap.py          # Bootstrap script with proper encoding
├── global_daily_update.py       # Daily incremental updater
├── run_global_bootstrap.py      # Easy menu-driven bootstrap runner
├── test_global_data.py          # Test script to verify data
├── global_streamlit_dashboard.py # Interactive web dashboard
├── requirements.txt             # Python dependencies
├── data/
│   └── global_opportunities.db  # SQLite database (Git LFS)
└── .github/
    └── workflows/
        └── update-global-sam-db.yml # GitHub Actions workflow
```

## 🔧 Usage

### Running Bootstrap

#### Option 1: Easy Menu (Recommended)
```bash
python run_global_bootstrap.py
```

#### Option 2: Command Line
```bash
# Quick start - last 2 years
python global_bootstrap.py --start-year 2023 --end-year 2025 --clear

# Specific region
python global_bootstrap.py --region AFRICA --start-year 2023 --end-year 2025

# Full historical data (takes 2-3 hours)
python global_bootstrap.py --start-year 1998 --end-year 2025 --clear
```

### Daily Updates
```bash
# Update recent data (last 14 days)
python global_daily_update.py --lookback-days 14
```

### Testing Data
```bash
# Verify data is loading correctly
python test_global_data.py
```

## 📊 Data Coverage

After successful bootstrap, you should see:

| Region | Expected Records | Countries |
|--------|-----------------|-----------|
| AFRICA | 10-20% of total | 54 |
| AMERICAS | 20-30% of total | 35+ |
| ASIA | 25-35% of total | 50+ |
| MIDDLE_EAST | 15-25% of total | 20 |
| EUROPE | 15-25% of total | 45+ |

## 🐛 Fixed Issues

This version includes critical fixes for:
- ✅ CSV encoding errors (removed `low_memory` parameter with Python engine)
- ✅ Unicode decode errors (proper encoding detection)
- ✅ Memory issues (chunked processing)
- ✅ Download failures (automatic S3 fallback)
- ✅ Country identification (comprehensive ISO3 mapping)

## 🚦 GitHub Actions

The repository includes automated daily updates via GitHub Actions:

```yaml
# .github/workflows/update-global-sam-db.yml
# Runs daily at 2 AM EST
# Automatically updates with new opportunities
```

To enable:
1. Go to Settings → Actions → General
2. Ensure Actions are enabled
3. The workflow will run automatically

## 📈 Dashboard Features

The Streamlit dashboard provides:
- **Regional Overview** - Tabs for each Spirit of America portfolio
- **Heat Maps** - Geographic visualization of opportunities
- **Statistics Sidebar** - Real-time counts by region and sub-region
- **Data Tables** - Searchable, sortable contract listings
- **Export Functions** - Download filtered data as CSV

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/YourFeature`)
3. Commit changes (`git commit -m 'Add YourFeature'`)
4. Push to branch (`git push origin feature/YourFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- Data provided by [SAM.gov](https://sam.gov)
- Built for [Spirit of America](https://spiritofamerica.org/)
- Dashboard powered by [Streamlit](https://streamlit.io)
- Automated with [GitHub Actions](https://github.com/features/actions)

## 📞 Support

For issues or questions:
1. Check the [Issues](https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker/issues) page
2. Run `python test_global_data.py` to diagnose problems
3. Create a new issue with error details

---

**Version 2.0.0** - December 2024  
**Status**: 🟢 Fully Operational  
**Built for Spirit of America** - Supporting U.S. military-led efforts worldwide