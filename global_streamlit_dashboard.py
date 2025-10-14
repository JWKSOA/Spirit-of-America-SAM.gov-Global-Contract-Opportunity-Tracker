#!/usr/bin/env python3
"""
global_streamlit_dashboard_improved.py - Enhanced Spirit of America Global Dashboard
Fixed heatmaps and comprehensive sidebar statistics
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Page config MUST be first Streamlit command
st.set_page_config(
    page_title="🌐 Spirit of America: SAM.gov Global Contract Opportunity Tracker",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"  # Changed to expanded
)

# Import global utilities
from global_sam_utils import GlobalCountryManager, GlobalConfig, GlobalDatabaseManager

# ============================================================================
# Initialize System
# ============================================================================

@st.cache_resource
def init_system():
    """Initialize global SAM data system (cached)"""
    try:
        config = GlobalConfig()
        country_manager = GlobalCountryManager()
        db_manager = GlobalDatabaseManager(config, country_manager)
        
        # Initialize database if needed
        if not config.db_path.exists():
            db_manager.initialize_database()
        
        return {
            'config': config,
            'country_manager': country_manager,
            'db_manager': db_manager
        }
    except Exception as e:
        st.error(f"Failed to initialize system: {e}")
        return None

# ============================================================================
# Data Loading Functions
# ============================================================================

@st.cache_data(ttl=300)
def load_complete_statistics() -> dict:
    """Load complete statistics for all regions and sub-regions"""
    system = init_system()
    if not system:
        return {}
    
    try:
        with system['db_manager'].get_connection() as conn:
            cur = conn.cursor()
            
            stats = {
                'total': 0,
                'by_region': {},
                'by_subregion': {}
            }
            
            # Total global count
            cur.execute("SELECT COUNT(*) FROM opportunities")
            stats['total'] = cur.fetchone()[0]
            
            # Count by region
            cur.execute("""
                SELECT Geographic_Region, COUNT(*) 
                FROM opportunities 
                WHERE Geographic_Region IS NOT NULL
                GROUP BY Geographic_Region
            """)
            stats['by_region'] = dict(cur.fetchall())
            
            # Count by sub-region with region grouping
            cur.execute("""
                SELECT Geographic_Region, Geographic_SubRegion, COUNT(*) 
                FROM opportunities 
                WHERE Geographic_Region IS NOT NULL 
                  AND Geographic_SubRegion IS NOT NULL
                GROUP BY Geographic_Region, Geographic_SubRegion
                ORDER BY Geographic_Region, Geographic_SubRegion
            """)
            
            for region, subregion, count in cur.fetchall():
                if region not in stats['by_subregion']:
                    stats['by_subregion'][region] = {}
                stats['by_subregion'][region][subregion] = count
            
            return stats
            
    except Exception as e:
        st.error(f"Error loading statistics: {e}")
        return {}

@st.cache_data(ttl=300)
def load_region_data_with_countries(region: str) -> pd.DataFrame:
    """Load data for a region with proper country identification"""
    system = init_system()
    if not system:
        return pd.DataFrame()
    
    try:
        with system['db_manager'].get_connection() as conn:
            query = """
                SELECT 
                    NoticeId,
                    Title,
                    "Department/Ind.Agency" as Department,
                    "Sub-Tier" as SubTier,
                    Office,
                    PostedDate,
                    PostedDate_normalized,
                    Type,
                    PopCountry,
                    PopCountry_ISO3,
                    Geographic_Region,
                    Geographic_SubRegion,
                    PopCity,
                    PopState,
                    Active,
                    ResponseDeadLine,
                    SetASide,
                    NaicsCode,
                    Link,
                    Description
                FROM opportunities
                WHERE Geographic_Region = ?
                ORDER BY PostedDate_normalized DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(region,))
            
            # Parse dates
            if not df.empty and 'PostedDate_normalized' in df.columns:
                df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate_normalized'], errors='coerce')
            
            return df
            
    except Exception as e:
        st.error(f"Error loading region data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_subregion_data(region: str, subregion: str) -> pd.DataFrame:
    """Load data for a specific sub-region"""
    system = init_system()
    if not system:
        return pd.DataFrame()
    
    try:
        with system['db_manager'].get_connection() as conn:
            query = """
                SELECT * FROM opportunities
                WHERE Geographic_Region = ?
                  AND Geographic_SubRegion = ?
                ORDER BY PostedDate_normalized DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(region, subregion))
            
            if not df.empty and 'PostedDate_normalized' in df.columns:
                df['PostedDate_parsed'] = pd.to_datetime(df['PostedDate_normalized'], errors='coerce')
            
            return df
            
    except Exception as e:
        st.error(f"Error loading subregion data: {e}")
        return pd.DataFrame()

# ============================================================================
# Visualization Functions
# ============================================================================

def create_enhanced_region_heatmap(df: pd.DataFrame, region: str) -> go.Figure:
    """Create enhanced heatmap with proper country data"""
    if df.empty:
        return go.Figure().add_annotation(
            text="No data available for heatmap",
            showarrow=False,
            font=dict(size=20)
        )
    
    # Group by ISO3 code for proper mapping
    if 'PopCountry_ISO3' in df.columns:
        country_summary = df[df['PopCountry_ISO3'].notna()].groupby('PopCountry_ISO3').agg({
            'NoticeId': 'count',
            'PopCountry': 'first'
        }).reset_index()
        country_summary.columns = ['ISO3', 'Opportunities', 'Country_Name']
    else:
        return go.Figure().add_annotation(
            text="Country data not properly formatted",
            showarrow=False,
            font=dict(size=20)
        )
    
    if country_summary.empty:
        return go.Figure().add_annotation(
            text="No country data found",
            showarrow=False,
            font=dict(size=20)
        )
    
    # Create choropleth map
    fig = px.choropleth(
        country_summary,
        locations='ISO3',
        locationmode='ISO-3',
        color='Opportunities',
        hover_name='Country_Name',
        hover_data={'Opportunities': ':,'},
        color_continuous_scale='YlOrRd',
        title=f'{region.replace("_", " ").title()} - Contract Opportunities by Country',
        labels={'Opportunities': 'Total Opportunities'}
    )
    
    # Set appropriate geographic scope
    scope_map = {
        'AFRICA': 'africa',
        'AMERICAS': 'americas', 
        'ASIA': 'asia',
        'MIDDLE_EAST': 'asia',
        'EUROPE': 'europe'
    }
    
    fig.update_geos(
        scope=scope_map.get(region, 'world'),
        showcoastlines=True,
        coastlinecolor='DarkGray',
        showland=True,
        landcolor='WhiteSmoke',
        showcountries=True,
        countrycolor='LightGray'
    )
    
    fig.update_layout(
        height=500,
        margin=dict(t=50, b=0, l=0, r=0),
        coloraxis_colorbar=dict(
            title="Opportunities",
            tickformat=",."
        )
    )
    
    return fig

def create_country_ranking_chart(df: pd.DataFrame, region: str, top_n: int = 10) -> go.Figure:
    """Create bar chart of top countries by opportunities"""
    if df.empty or 'PopCountry' not in df.columns:
        return go.Figure()
    
    # Count opportunities by country
    country_counts = df['PopCountry'].value_counts().head(top_n)
    
    if country_counts.empty:
        return go.Figure()
    
    # Create bar chart
    fig = px.bar(
        x=country_counts.values,
        y=country_counts.index,
        orientation='h',
        title=f'Top {top_n} Countries by Contract Opportunities',
        labels={'x': 'Number of Opportunities', 'y': 'Country'},
        color=country_counts.values,
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        height=400,
        showlegend=False,
        margin=dict(t=50, b=20, l=100, r=20)
    )
    
    return fig

def create_timeline_chart(df: pd.DataFrame, title: str) -> go.Figure:
    """Create timeline chart for opportunities"""
    if df.empty or 'PostedDate_parsed' not in df.columns:
        return go.Figure()
    
    valid_dates = df[df['PostedDate_parsed'].notna()]
    if valid_dates.empty:
        return go.Figure()
    
    # Group by date
    timeline = valid_dates.groupby(valid_dates['PostedDate_parsed'].dt.date).size().reset_index()
    timeline.columns = ['Date', 'Count']
    
    fig = px.line(
        timeline, 
        x='Date', 
        y='Count', 
        title=title,
        labels={'Count': 'Opportunities Posted'}
    )
    
    fig.update_traces(mode='lines+markers', line=dict(width=2))
    fig.update_layout(
        height=350,
        margin=dict(t=50, b=20, l=20, r=20),
        hovermode='x unified'
    )
    
    return fig

def create_subregion_pie_chart(stats: dict, region: str) -> go.Figure:
    """Create pie chart for sub-region distribution"""
    if region not in stats or not stats[region]:
        return go.Figure()
    
    subregion_data = stats[region]
    
    fig = px.pie(
        values=list(subregion_data.values()),
        names=list(subregion_data.keys()),
        title="Distribution by Sub-Region",
        hole=0.4  # Make it a donut chart
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>' +
                     'Opportunities: %{value:,}<br>' +
                     'Percentage: %{percent}<br>' +
                     '<extra></extra>'
    )
    
    fig.update_layout(
        height=400,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.05
        )
    )
    
    return fig

# ============================================================================
# Sidebar Statistics Display
# ============================================================================

def display_comprehensive_sidebar_stats():
    """Display comprehensive statistics in sidebar as requested"""
    stats = load_complete_statistics()
    
    if not stats:
        st.sidebar.error("Unable to load statistics")
        return
    
    # Global total
    st.sidebar.markdown("## 🌐 Global Statistics")
    st.sidebar.metric("**TOTAL GLOBAL CONTRACTS**", f"{stats.get('total', 0):,}")
    
    st.sidebar.markdown("---")
    
    # Display each region with sub-regions
    regions_config = {
        'AFRICA': ('🌍', 'Africa'),
        'AMERICAS': ('🌎', 'Americas'),
        'ASIA': ('🌏', 'Asia'),
        'MIDDLE_EAST': ('🌐', 'Middle East'),
        'EUROPE': ('🇪🇺', 'Europe')
    }
    
    for region_key, (emoji, display_name) in regions_config.items():
        region_total = stats.get('by_region', {}).get(region_key, 0)
        
        # Region header with total
        st.sidebar.markdown(f"### {emoji} **{display_name}**")
        st.sidebar.markdown(f"**Total Contracts: {region_total:,}**")
        
        # Sub-regions breakdown
        if region_key in stats.get('by_subregion', {}):
            subregions = stats['by_subregion'][region_key]
            
            # Sort sub-regions by count (descending)
            sorted_subregions = sorted(subregions.items(), key=lambda x: x[1], reverse=True)
            
            # Display each sub-region
            for idx, (subregion, count) in enumerate(sorted_subregions, 1):
                st.sidebar.markdown(f"({idx}) **{subregion}:** {count:,} contracts")
        
        st.sidebar.markdown("")  # Add spacing between regions

# ============================================================================
# Main Region Display
# ============================================================================

def display_enhanced_region_overview(region: str):
    """Display enhanced overview with working heatmap and statistics"""
    
    # Load data
    df_region = load_region_data_with_countries(region)
    
    if df_region.empty:
        st.warning(f"No data available for {region.replace('_', ' ').title()}")
        return
    
    # Display metrics
    st.markdown(f"### 📊 {region.replace('_', ' ').title()} Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total = len(df_region)
        st.metric("Total Opportunities", f"{total:,}")
    
    with col2:
        unique_countries = df_region['PopCountry'].nunique() if 'PopCountry' in df_region.columns else 0
        st.metric("Countries", f"{unique_countries}")
    
    with col3:
        unique_agencies = df_region['Department'].nunique() if 'Department' in df_region.columns else 0
        st.metric("Agencies", f"{unique_agencies}")
    
    with col4:
        # Recent activity (last 30 days)
        if 'PostedDate_parsed' in df_region.columns:
            recent = df_region[
                df_region['PostedDate_parsed'] >= (datetime.now() - timedelta(days=30))
            ]
            st.metric("Last 30 Days", f"{len(recent):,}")
        else:
            st.metric("Last 30 Days", "N/A")
    
    # Visualizations
    st.markdown("---")
    
    # Create two columns for main visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Enhanced heatmap
        st.markdown("#### 🗺️ Geographic Distribution")
        heatmap_fig = create_enhanced_region_heatmap(df_region, region)
        st.plotly_chart(heatmap_fig, use_container_width=True)
    
    with col2:
        # Top countries bar chart
        st.markdown("#### 🏆 Top Countries")
        ranking_fig = create_country_ranking_chart(df_region, region)
        st.plotly_chart(ranking_fig, use_container_width=True)
    
    # Timeline and pie chart
    col3, col4 = st.columns(2)
    
    with col3:
        # Timeline
        timeline_fig = create_timeline_chart(
            df_region, 
            f"Daily Postings - {region.replace('_', ' ').title()}"
        )
        st.plotly_chart(timeline_fig, use_container_width=True)
    
    with col4:
        # Sub-region distribution
        stats = load_complete_statistics()
        if stats.get('by_subregion', {}).get(region):
            pie_fig = create_subregion_pie_chart(stats['by_subregion'], region)
            st.plotly_chart(pie_fig, use_container_width=True)
    
    # Country statistics table
    st.markdown("---")
    st.markdown("#### 📋 Country Statistics")
    
    if 'PopCountry' in df_region.columns:
        country_stats = df_region.groupby('PopCountry').agg({
            'NoticeId': 'count',
            'Department': 'nunique'
        }).reset_index()
        country_stats.columns = ['Country', 'Total Opportunities', 'Unique Agencies']
        country_stats = country_stats.sort_values('Total Opportunities', ascending=False).head(20)
        
        st.dataframe(
            country_stats,
            hide_index=True,
            use_container_width=True,
            column_config={
                'Total Opportunities': st.column_config.NumberColumn(format="%d"),
                'Unique Agencies': st.column_config.NumberColumn(format="%d")
            }
        )

def display_subregion_content(region: str, subregion: str):
    """Display content for a specific sub-region"""
    df = load_subregion_data(region, subregion)
    
    if df.empty:
        st.warning(f"No data available for {subregion}")
        return
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Opportunities", f"{len(df):,}")
    with col2:
        unique_countries = df['PopCountry'].nunique() if 'PopCountry' in df.columns else 0
        st.metric("Countries", f"{unique_countries}")
    with col3:
        unique_agencies = df['Department'].nunique() if 'Department' in df.columns else 0
        st.metric("Agencies", f"{unique_agencies}")
    with col4:
        if 'PostedDate_parsed' in df.columns and df['PostedDate_parsed'].notna().any():
            latest = df['PostedDate_parsed'].max()
            st.metric("Latest Post", latest.strftime("%Y-%m-%d") if pd.notna(latest) else "N/A")
    
    # Country distribution chart
    if 'PopCountry' in df.columns:
        country_counts = df['PopCountry'].value_counts().head(10)
        if not country_counts.empty:
            fig = px.bar(
                x=country_counts.values,
                y=country_counts.index,
                orientation='h',
                title=f"Top Countries in {subregion}",
                labels={'x': 'Opportunities', 'y': 'Country'},
                color=country_counts.values,
                color_continuous_scale='Viridis'
            )
            fig.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    # Data table
    st.markdown("### 📋 Recent Contract Opportunities")
    
    display_cols = ['PostedDate', 'Title', 'Department', 'PopCountry', 'Type', 'Link']
    available_cols = [col for col in display_cols if col in df.columns]
    
    if available_cols:
        display_df = df[available_cols].head(100).copy()
        display_df = display_df.fillna('')
        
        if 'Link' in display_df.columns:
            st.dataframe(
                display_df,
                column_config={
                    "Link": st.column_config.LinkColumn(
                        "SAM.gov Link",
                        help="Click to view opportunity",
                        display_text="View"
                    )
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.dataframe(display_df, hide_index=True, use_container_width=True)
        
        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            "📥 Download Full Data",
            csv,
            f"sam_{region}_{subregion.replace(' ', '_')}.csv",
            "text/csv"
        )

# ============================================================================
# Main Application
# ============================================================================

def main():
    """Main dashboard application"""
    
    # Initialize system
    system = init_system()
    if not system:
        st.error("❌ Failed to initialize system")
        st.stop()
    
    # Title and header
    st.title("🌐 Spirit of America: SAM.gov Global Contract Opportunity Tracker")
    st.markdown("*Tracking U.S. government contracting opportunities worldwide for Spirit of America portfolios*")
    
    # Display current date
    st.caption(f"📅 Dashboard Updated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    
    # Display comprehensive sidebar statistics
    display_comprehensive_sidebar_stats()
    
    # Add refresh button to sidebar
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Refresh Dashboard", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    # Add info to sidebar
    st.sidebar.markdown("---")
    st.sidebar.caption("""
    **Data Source:** SAM.gov  
    **Coverage:** 195+ Countries  
    **Updates:** Daily at 2 AM EST  
    
    *Built for Spirit of America*
    """)
    
    # Create tabs for each geographic region
    region_tabs = st.tabs([
        "🌍 Africa",
        "🌎 Americas", 
        "🌏 Asia",
        "🌐 Middle East",
        "🇪🇺 Europe"
    ])
    
    # Map tab names to region keys
    region_map = {
        0: "AFRICA",
        1: "AMERICAS",
        2: "ASIA",
        3: "MIDDLE_EAST",
        4: "EUROPE"
    }
    
    # Process each region tab
    for idx, region_tab in enumerate(region_tabs):
        with region_tab:
            region_key = region_map[idx]
            
            # Display enhanced region overview with working heatmap
            display_enhanced_region_overview(region_key)
            
            # Get sub-regions for this region
            subregions = list(system['country_manager'].GEOGRAPHIC_REGIONS[region_key].keys())
            
            if subregions:
                st.markdown("---")
                st.markdown(f"### 📂 {region_key.replace('_', ' ').title()} Sub-Regions")
                
                # Create tabs for sub-regions
                subregion_tabs = st.tabs([f"📍 {sr}" for sr in subregions])
                
                for sub_idx, sub_tab in enumerate(subregion_tabs):
                    with sub_tab:
                        subregion_name = subregions[sub_idx]
                        display_subregion_content(region_key, subregion_name)

if __name__ == "__main__":
    main()