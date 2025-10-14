#!/usr/bin/env python3
"""
global_streamlit_dashboard.py - Spirit of America SAM.gov Global Contract Opportunity Tracker
Organized by Spirit of America's geographic portfolios
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
    initial_sidebar_state="collapsed"
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
def load_region_data(region: str, days_back: int = None) -> pd.DataFrame:
    """Load data for a specific region"""
    system = init_system()
    if not system:
        return pd.DataFrame()
    
    try:
        with system['db_manager'].get_connection() as conn:
            # Build query
            base_query = """
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
                    AwardNumber,
                    AwardDate,
                    "Award$" as AwardAmount,
                    Awardee,
                    Link,
                    Description,
                    PrimaryContactFullName,
                    PrimaryContactEmail,
                    PrimaryContactPhone
                FROM opportunities
                WHERE Geographic_Region = ?
            """
            
            params = [region]
            
            if days_back:
                cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
                base_query += " AND PostedDate_normalized >= ?"
                params.append(cutoff)
            
            base_query += " ORDER BY PostedDate_normalized DESC"
            
            df = pd.read_sql_query(base_query, conn, params=params)
            
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

@st.cache_data(ttl=300)
def get_region_statistics(region: str) -> dict:
    """Get statistics for a specific region"""
    system = init_system()
    if not system:
        return {}
    
    try:
        with system['db_manager'].get_connection() as conn:
            cur = conn.cursor()
            
            stats = {}
            
            # Total opportunities
            cur.execute("""
                SELECT COUNT(*) FROM opportunities
                WHERE Geographic_Region = ?
            """, (region,))
            stats['total'] = cur.fetchone()[0]
            
            # By time periods
            today = datetime.now().date().isoformat()
            
            for days, label in [(7, 'last_7_days'), (30, 'last_30_days'), 
                               (365, 'last_year'), (1825, 'last_5_years')]:
                cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
                cur.execute("""
                    SELECT COUNT(*) FROM opportunities
                    WHERE Geographic_Region = ?
                      AND PostedDate_normalized >= ?
                      AND PostedDate_normalized <= ?
                """, (region, cutoff, today))
                stats[label] = cur.fetchone()[0]
            
            # By sub-region
            cur.execute("""
                SELECT Geographic_SubRegion, COUNT(*) 
                FROM opportunities
                WHERE Geographic_Region = ?
                  AND Geographic_SubRegion IS NOT NULL
                GROUP BY Geographic_SubRegion
            """, (region,))
            stats['by_subregion'] = dict(cur.fetchall())
            
            # By country
            cur.execute("""
                SELECT PopCountry, COUNT(*) 
                FROM opportunities
                WHERE Geographic_Region = ?
                  AND PopCountry IS NOT NULL
                GROUP BY PopCountry
                ORDER BY COUNT(*) DESC
                LIMIT 10
            """, (region,))
            stats['top_countries'] = dict(cur.fetchall())
            
            return stats
            
    except Exception as e:
        st.error(f"Error getting statistics: {e}")
        return {}

# ============================================================================
# Visualization Functions
# ============================================================================

def create_region_heatmap(df: pd.DataFrame, region: str) -> go.Figure:
    """Create heatmap for a specific region"""
    if df.empty or 'PopCountry_ISO3' not in df.columns:
        return go.Figure()
    
    # Group by country
    summary = df.groupby('PopCountry_ISO3').size().reset_index(name='Opportunities')
    
    # Define scope by region
    scope_map = {
        'AFRICA': 'africa',
        'AMERICAS': 'americas',
        'ASIA': 'asia',
        'MIDDLE_EAST': 'asia',  # Use asia scope for Middle East
        'EUROPE': 'europe'
    }
    
    fig = px.choropleth(
        summary,
        locations='PopCountry_ISO3',
        locationmode='ISO-3',
        color='Opportunities',
        hover_name='PopCountry_ISO3',
        color_continuous_scale='Viridis',
        title=f'{region.replace("_", " ").title()} Contract Opportunities Heatmap'
    )
    
    # Set appropriate scope
    scope = scope_map.get(region, 'world')
    fig.update_geos(
        scope=scope,
        showcoastlines=True,
        coastlinecolor='RebeccaPurple',
        showland=True,
        landcolor='LightGray'
    )
    
    fig.update_layout(height=500, margin=dict(t=40, b=0, l=0, r=0))
    return fig

def create_subregion_pie_chart(stats: dict, subregion_key: str) -> go.Figure:
    """Create pie chart for sub-region distribution"""
    if 'by_subregion' not in stats or not stats['by_subregion']:
        return go.Figure()
    
    subregion_data = stats['by_subregion']
    
    fig = px.pie(
        values=list(subregion_data.values()),
        names=list(subregion_data.keys()),
        title="Distribution by Sub-Region"
    )
    
    fig.update_layout(height=400)
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
    
    fig = px.line(timeline, x='Date', y='Count', title=title)
    fig.update_traces(mode='lines+markers')
    fig.update_layout(height=350, margin=dict(t=40, b=20, l=20, r=20))
    return fig

# ============================================================================
# Display Functions
# ============================================================================

def display_region_overview(region: str):
    """Display overview for a geographic region"""
    # Get statistics
    stats = get_region_statistics(region)
    
    # Display metrics
    st.markdown(f"### 📊 {region.replace('_', ' ').title()} Overview")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Opportunities", f"{stats.get('total', 0):,}")
    with col2:
        st.metric("Last 7 Days", f"{stats.get('last_7_days', 0):,}")
    with col3:
        st.metric("Last 30 Days", f"{stats.get('last_30_days', 0):,}")
    with col4:
        st.metric("Last Year", f"{stats.get('last_year', 0):,}")
    with col5:
        st.metric("Last 5 Years", f"{stats.get('last_5_years', 0):,}")
    
    # Load region data
    df_region = load_region_data(region)
    
    if not df_region.empty:
        # Create two columns for visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            # Regional heatmap
            heatmap_fig = create_region_heatmap(df_region, region)
            st.plotly_chart(heatmap_fig, use_container_width=True)
        
        with col2:
            # Sub-region distribution
            if stats.get('by_subregion'):
                pie_fig = create_subregion_pie_chart(stats, 'by_subregion')
                st.plotly_chart(pie_fig, use_container_width=True)
        
        # Timeline chart
        timeline_fig = create_timeline_chart(df_region, f"{region.replace('_', ' ').title()} Daily Postings")
        st.plotly_chart(timeline_fig, use_container_width=True)
        
        # Top countries table
        if stats.get('top_countries'):
            st.markdown("### 🏆 Top Countries by Opportunities")
            countries_df = pd.DataFrame(
                list(stats['top_countries'].items()),
                columns=['Country', 'Opportunities']
            )
            st.dataframe(countries_df, hide_index=True, use_container_width=True)
    else:
        st.info(f"No data available for {region.replace('_', ' ').title()}")

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
    
    # Pie chart of countries in sub-region
    if 'PopCountry' in df.columns:
        country_counts = df['PopCountry'].value_counts().head(10)
        fig = px.pie(
            values=country_counts.values,
            names=country_counts.index,
            title=f"Top Countries in {subregion}"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Data table
    st.markdown("### 📋 Contract Opportunities")
    
    # Prepare display columns
    display_cols = ['PostedDate', 'Title', 'Department', 'PopCountry', 'Type', 'Link']
    available_cols = [col for col in display_cols if col in df.columns]
    
    if available_cols:
        display_df = df[available_cols].head(100).copy()
        display_df = display_df.fillna('')
        
        # Configure table with clickable links
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
    
    # Create tabs for each geographic region (parent tabs)
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
            
            # Display region overview
            display_region_overview(region_key)
            
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
    
    # Sidebar with global statistics
    with st.sidebar:
        st.header("🌐 Global Statistics")
        
        # Get global stats
        try:
            stats = system['db_manager'].get_statistics_by_region()
            
            st.metric("Total Opportunities", f"{stats.get('total_records', 0):,}")
            
            if stats.get('by_region'):
                st.markdown("### By Region")
                for region, count in stats['by_region'].items():
                    if region:
                        st.metric(region.replace('_', ' ').title(), f"{count:,}")
            
            # Refresh button
            st.markdown("---")
            if st.button("🔄 Refresh Dashboard", use_container_width=True):
                st.cache_data.clear()
                st.rerun()
            
            # Info
            st.markdown("---")
            st.caption("""
            **Data Source:** SAM.gov  
            **Coverage:** Global  
            **Updates:** Daily  
            
            *For Spirit of America*
            """)
            
        except Exception as e:
            st.error(f"Error loading statistics: {e}")

if __name__ == "__main__":
    main()