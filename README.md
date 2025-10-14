# 🌐 Spirit of America: SAM.gov Global Contract Opportunity Tracker

[![Daily Update](https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker/actions/workflows/update-global-sam-db.yml/badge.svg)](https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker/actions/workflows/update-global-sam-db.yml)
[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://your-app-name.streamlit.app)

An automated dashboard for tracking U.S. government contract opportunities worldwide, organized by Spirit of America's geographic portfolios.

## 🌍 Overview

This system automatically collects, processes, and visualizes contract opportunities from SAM.gov for all countries globally, organized into Spirit of America's five geographic portfolios:

- **Africa** (54 countries across 5 sub-regions)
- **Americas** (35+ countries across 4 sub-regions)  
- **Asia** (50+ countries across 4 sub-regions including Oceania)
- **Middle East** (20 countries across 2 sub-regions)
- **Europe** (45+ countries across 4 sub-regions)

## ✨ Features

### 🎯 Core Functionality
- **Automated Data Collection**: Daily synchronization with SAM.gov
- **Global Coverage**: Tracks opportunities in 195+ countries
- **Smart Categorization**: Automatic assignment to Spirit of America portfolios
- **Historical Archives**: Data from FY1998 to present
- **Real-time Updates**: GitHub Actions workflow runs nightly

### 📊 Dashboard Features
- **Geographic Heat Maps**: Visual representation by region and country
- **Sub-regional Analysis**: Detailed breakdown by Spirit of America's sub-portfolios
- **Temporal Trends**: Track opportunities over time
- **Interactive Tables**: Searchable, sortable data with direct SAM.gov links
- **Export Capabilities**: Download filtered data as CSV

### 🔧 Technical Features
- **Incremental Updates**: Only processes new/changed data
- **Intelligent Country Recognition**: Handles variations in country names and codes
- **Database Optimization**: Indexed for fast queries
- **Error Recovery**: Automatic retries and fallback mechanisms
- **Progress Tracking**: Resume capability for large downloads

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- 2GB RAM minimum
- 500MB disk space

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker.git
cd Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker

# Install dependencies
pip install -r requirements.txt

# Initialize database with recent data (quick, ~10 minutes)
python global_bootstrap.py --start-year 2020

# Or load full historical data (slower, ~2-3 hours)
python global_bootstrap.py --full-history

# Run the dashboard
streamlit run global_streamlit_dashboard.py
```

## 📁 Project Structure

```
├── global_sam_utils.py           # Core utilities and country management
├── global_streamlit_dashboard.py # Interactive web dashboard
├── global_bootstrap.py           # Initial data loader
├── global_daily_update.py        # Daily incremental updater
├── requirements.txt              # Python dependencies
├── data/
│   └── global_opportunities.db   # SQLite database
├── .github/
│   └── workflows/
│       └── update-global-sam-db.yml  # Automated updates
└── .streamlit/
    └── config.toml              # Dashboard configuration
```

## 🌍 Geographic Organization

### Africa
- **Northern Africa**: Algeria, Egypt, Libya, Morocco, Sudan, Tunisia, etc.
- **Eastern Africa**: Ethiopia, Kenya, Tanzania, Uganda, Rwanda, etc.
- **Middle Africa**: Angola, Cameroon, DRC, Chad, etc.
- **Southern Africa**: South Africa, Botswana, Zimbabwe, etc.
- **Western Africa**: Nigeria, Ghana, Senegal, Mali, etc.

### Americas
- **Caribbean**: 25+ island nations and territories
- **Central America**: 8 countries from Mexico to Panama
- **Northern America**: USA, Canada, Greenland, Bermuda
- **South America**: 14 countries including Brazil, Argentina, Colombia

### Asia
- **Eastern Asia**: China, Japan, Korea, Mongolia, Taiwan
- **Southern Asia**: India, Pakistan, Bangladesh, Afghanistan, etc.
- **South-Eastern Asia**: Indonesia, Philippines, Vietnam, Thailand, etc.
- **Oceania**: Australia, New Zealand, Pacific Islands

### Middle East
- **Near-East**: Turkey, Iraq, Israel, Jordan, Syria, etc.
- **Far-East**: Saudi Arabia, UAE, Iran, Central Asian states

### Europe
- **Eastern Europe**: Poland, Ukraine, Czech Republic, etc.
- **Northern Europe**: UK, Scandinavia, Baltic states
- **Southern Europe**: Italy, Spain, Greece, Balkans
- **Western Europe**: Germany, France, Benelux, Switzerland

## 📈 Data Sources & Updates

- **Primary Source**: [SAM.gov Contract Opportunities](https://sam.gov/data-services/Contract%20Opportunities/datagov)
- **Update Frequency**: Daily at 2:00 AM EST
- **Data Coverage**: FY1998 to present
- **Typical Volume**: 10,000+ opportunities globally

## 🔧 Configuration

### Streamlit Configuration
Edit `.streamlit/config.toml` to customize appearance:

```toml
[theme]
primaryColor = "#FF6B6B"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
```

### Database Location
Default: `data/global_opportunities.db`

To change, edit `GlobalConfig` in `global_sam_utils.py`

## 🚦 Deployment

### Deploy to Streamlit Cloud

1. Fork this repository
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your GitHub account
4. Select repository and branch
5. Set main file: `global_streamlit_dashboard.py`
6. Click Deploy

### GitHub Actions Setup

The workflow runs automatically. To configure:

1. Go to Settings → Actions → General
2. Ensure Actions are enabled
3. Check workflow permissions (read/write)

## 📊 Usage Examples

### View Regional Opportunities
1. Click on a geographic region tab (e.g., "Africa")
2. View heat map and statistics
3. Navigate to sub-region tabs for detailed data

### Filter by Time Period
- Regional overview shows data for multiple time periods
- Statistics update automatically

### Export Data
1. Navigate to any data table
2. Use search/filter options
3. Click "Download CSV" button

### Search Specific Countries
1. Go to relevant region tab
2. Navigate to sub-region
3. Use table search functionality

## 🛠️ Maintenance

### Manual Update
```bash
python global_daily_update.py
```

### Add Historical Years
```bash
python global_bootstrap.py --start-year 1998 --end-year 2019
```

### Database Optimization
```bash
python -c "from global_sam_utils import GlobalDatabaseManager, GlobalConfig; 
db = GlobalDatabaseManager(GlobalConfig(), None); 
db.optimize_database()"
```

## 📈 Performance

- **Daily Updates**: <5 minutes
- **Dashboard Load**: <3 seconds
- **Database Size**: ~200-500MB
- **Query Response**: <1 second

## 🐛 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Import errors | `pip install -r requirements.txt --upgrade` |
| Database not found | `python global_bootstrap.py --start-year 2023` |
| Streamlit port conflict | `lsof -i :8501` then `kill -9 [PID]` |
| GitHub Actions failing | Check repository settings and workflow permissions |

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Data provided by [SAM.gov](https://sam.gov)
- Built for [Spirit of America](https://spiritofamerica.org/)
- Powered by [Streamlit](https://streamlit.io)
- Automated with [GitHub Actions](https://github.com/features/actions)

## 📞 Support

For issues or questions:
1. Check the [Issues](https://github.com/YOUR_USERNAME/Spirit-of-America-SAM.gov-Global-Contract-Opportunity-Tracker/issues) page
2. Review the documentation
3. Create a new issue with details

---

**Built for Spirit of America** | Supporting U.S. military-led efforts worldwide

*Last Updated: 2024 | Version: 1.0.0 | Status: 🟢 Active*