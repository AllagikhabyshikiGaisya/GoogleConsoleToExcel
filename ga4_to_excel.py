import os
import json
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GA4SheetsSync:
    def __init__(self, property_id: str, sheet_id: str):
        """Initialize the GA4 to Sheets sync class"""
        self.property_id = property_id
        self.sheet_id = sheet_id
        self.credentials = None
        self.ga4_client = None
        self.sheets_client = None
        
    def get_credentials(self) -> service_account.Credentials:
        """Get credentials from environment variable or local file"""
        logger.info("🔐 Getting credentials...")
        
        # First try environment variable (for deployment)
        creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
        if creds_json:
            logger.info("Using credentials from environment variable")
            try:
                creds_dict = json.loads(creds_json)
                return service_account.Credentials.from_service_account_info(creds_dict)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing GOOGLE_CREDENTIALS_JSON: {e}")
                raise
        
        # Try local file (for development)
        local_paths = [
            "credentials.json",
            "service_account.json",
            "C:\\GA4_Project\\credentials.json",
            os.path.join(os.getcwd(), "credentials.json")
        ]
        
        for path in local_paths:
            if os.path.exists(path):
                logger.info(f"Using local credentials file: {path}")
                return service_account.Credentials.from_service_account_file(path)
        
        raise ValueError("❌ No credentials found. Set GOOGLE_CREDENTIALS_JSON environment variable or place credentials.json in project folder")
    
    def setup_clients(self):
        """Setup GA4 and Google Sheets clients"""
        logger.info("🔧 Setting up clients...")
        
        # Get credentials
        self.credentials = self.get_credentials()
        
        # Setup GA4 client
        self.ga4_client = BetaAnalyticsDataClient(credentials=self.credentials)
        logger.info("✅ GA4 client authenticated")
        
        # Setup Google Sheets client
        sheets_scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        sheets_creds = self.credentials.with_scopes(sheets_scopes)
        self.sheets_client = gspread.authorize(sheets_creds)
        logger.info("✅ Google Sheets client authenticated")
    
    def fetch_ga4_data(self, start_date: str = "today", end_date: str = "today", 
                      dimensions: List[str] = None, metrics: List[str] = None) -> pd.DataFrame:
        """Fetch data from GA4 API"""
        logger.info(f"📊 Fetching GA4 data for {start_date} to {end_date}")
        
        # Default dimensions and metrics
        if dimensions is None:
            dimensions = ["date", "country", "deviceCategory", "sessionSource"]
        
        if metrics is None:
            metrics = [
                "sessions", 
                "totalUsers", 
                "newUsers", 
                "bounceRate", 
                "averageSessionDuration",
                "screenPageViews",
                "conversions"
            ]
        
        try:
            # Build the request
            request = RunReportRequest(
                property=f"properties/{self.property_id}",
                dimensions=[Dimension(name=d) for d in dimensions],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                keep_empty_rows=False,
                return_property_quota=True
            )
            
            # Run the request
            response = self.ga4_client.run_report(request)
            
            if not response.rows:
                logger.warning("⚠️ No data returned from GA4 API")
                return pd.DataFrame()
            
            # Convert to DataFrame
            rows = []
            for row in response.rows:
                row_data = []
                
                # Add dimension values
                for dim_value in row.dimension_values:
                    row_data.append(dim_value.value)
                
                # Add metric values
                for metric_value in row.metric_values:
                    row_data.append(metric_value.value)
                
                rows.append(row_data)
            
            # Create DataFrame
            columns = dimensions + metrics
            df = pd.DataFrame(rows, columns=columns)
            
            # Clean and format the data
            df = self.format_dataframe(df, dimensions, metrics)
            
            logger.info(f"📈 Fetched {len(df)} rows of data")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error fetching GA4 data: {e}")
            raise
    
    def format_dataframe(self, df: pd.DataFrame, dimensions: List[str], metrics: List[str]) -> pd.DataFrame:
        """Format the DataFrame with proper data types and formatting"""
        if df.empty:
            return df
        
        # Format date if present
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Convert metrics to proper types
        for metric in metrics:
            if metric in ['bounceRate', 'averageSessionDuration']:
                df[metric] = pd.to_numeric(df[metric], errors='coerce').round(4)
            else:
                df[metric] = pd.to_numeric(df[metric], errors='coerce').fillna(0).astype('Int64')
        
        # Add metadata
        df.insert(0, 'last_updated', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        df.insert(1, 'data_freshness', 'live')
        
        return df
    
    def get_existing_sheet_data(self, worksheet) -> pd.DataFrame:
        """Get existing data from Google Sheet"""
        try:
            logger.info("🔍 Reading existing sheet data...")
            existing_df = get_as_dataframe(worksheet, evaluate_formulas=True)
            
            # Clean up the DataFrame
            existing_df = existing_df.dropna(how='all')  # Drop empty rows
            existing_df = existing_df.dropna(axis=1, how='all')  # Drop empty columns
            
            if existing_df.empty:
                logger.info("📭 Sheet is empty")
                return pd.DataFrame()
            
            logger.info(f"📊 Found {len(existing_df)} existing rows")
            return existing_df
            
        except Exception as e:
            logger.warning(f"⚠️ Could not read existing data: {e}")
            return pd.DataFrame()
    
    def update_google_sheet(self, df: pd.DataFrame, worksheet_name: str = None):
        """Update Google Sheet with new data"""
        if df.empty:
            logger.warning("⚠️ No data to update")
            return
        
        try:
            # Open the sheet
            sheet = self.sheets_client.open_by_key(self.sheet_id)
            
            if worksheet_name:
                try:
                    worksheet = sheet.worksheet(worksheet_name)
                except gspread.WorksheetNotFound:
                    logger.info(f"Creating new worksheet: {worksheet_name}")
                    worksheet = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            else:
                worksheet = sheet.sheet1
            
            logger.info(f"📋 Opened Google Sheet: '{sheet.title}'")
            
            # Get existing data
            existing_df = self.get_existing_sheet_data(worksheet)
            
            # Check for duplicates if we have date column
            if not existing_df.empty and 'date' in df.columns and 'date' in existing_df.columns:
                new_dates = set(df['date'].unique())
                existing_dates = set(existing_df['date'].unique())
                duplicate_dates = new_dates.intersection(existing_dates)
                
                if duplicate_dates:
                    logger.info(f"🔄 Found duplicate dates: {duplicate_dates}")
                    # Remove duplicates from existing data
                    existing_df = existing_df[~existing_df['date'].isin(duplicate_dates)]
            
            # Combine data (new data first)
            if existing_df.empty:
                final_df = df
                logger.info("📝 Writing new data to empty sheet")
            else:
                final_df = pd.concat([df, existing_df], ignore_index=True)
                logger.info(f"📝 Combining {len(df)} new rows with {len(existing_df)} existing rows")
            
            # Sort by date if available (newest first)
            if 'date' in final_df.columns:
                final_df = final_df.sort_values('date', ascending=False)
            
            # Clear and write data
            worksheet.clear()
            set_with_dataframe(worksheet, final_df, include_index=False)
            
            # Format header
            self.format_sheet_header(worksheet)
            
            logger.info(f"✅ Successfully updated Google Sheet with {len(final_df)} total rows")
            
        except Exception as e:
            logger.error(f"❌ Error updating Google Sheet: {e}")
            raise
    
    def format_sheet_header(self, worksheet):
        """Format the header row of the sheet"""
        try:
            worksheet.format("1:1", {
                "backgroundColor": {"red": 0.2, "green": 0.6, "blue": 1.0},
                "textFormat": {
                    "bold": True, 
                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
                },
                "horizontalAlignment": "CENTER"
            })
            
            # Auto-resize columns
            worksheet.columns_auto_resize(0, worksheet.col_count)
            
        except Exception as e:
            logger.warning(f"⚠️ Header formatting failed: {e}")
    
    def sync_data(self, start_date: str = "today", end_date: str = "today", 
                 dimensions: List[str] = None, metrics: List[str] = None,
                 worksheet_name: str = None):
        """Main sync function"""
        logger.info("🚀 Starting GA4 to Google Sheets sync...")
        
        try:
            # Setup clients
            self.setup_clients()
            
            # Fetch GA4 data
            df = self.fetch_ga4_data(start_date, end_date, dimensions, metrics)
            
            if df.empty:
                logger.info("📭 No data to sync")
                return
            
            # Update Google Sheet
            self.update_google_sheet(df, worksheet_name)
            
            logger.info("🎉 Sync completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")
            raise

def main():
    """Main function with configuration"""
    
    # Configuration - You can modify these values
    CONFIG = {
        'PROPERTY_ID': os.getenv('GA4_PROPERTY_ID', '495544746'),  # Your GA4 Property ID
        'SHEET_ID': os.getenv('GOOGLE_SHEET_ID', '1kD1t4h48fcSNELqpyY9pE6S4yJn6ZLamYva09HgyPRM'),  # Your Google Sheet ID
        'START_DATE': 'today',  # Options: 'today', 'yesterday', '7daysAgo', '30daysAgo', 'YYYY-MM-DD'
        'END_DATE': 'today',    # Options: 'today', 'yesterday', '7daysAgo', '30daysAgo', 'YYYY-MM-DD'
        'WORKSHEET_NAME': None,  # None for first sheet, or specify sheet name
        
        # Dimensions to fetch (customize as needed)
        'DIMENSIONS': [
            'date',
            'country',
            'deviceCategory',
            'sessionSource',
            'sessionMedium'
        ],
        
        # Metrics to fetch (customize as needed)
        'METRICS': [
            'sessions',
            'totalUsers',
            'newUsers',
            'bounceRate',
            'averageSessionDuration',
            'screenPageViews',
            'conversions',
            'totalRevenue'
        ]
    }
    
    print("=" * 60)
    print("🚀 GA4 TO GOOGLE SHEETS LIVE DATA SYNC")
    print("=" * 60)
    print(f"📊 GA4 Property ID: {CONFIG['PROPERTY_ID']}")
    print(f"📋 Google Sheet ID: {CONFIG['SHEET_ID']}")
    print(f"📅 Date Range: {CONFIG['START_DATE']} to {CONFIG['END_DATE']}")
    print(f"📈 Dimensions: {', '.join(CONFIG['DIMENSIONS'])}")
    print(f"📊 Metrics: {', '.join(CONFIG['METRICS'])}")
    print("=" * 60)
    
    try:
        # Initialize the sync class
        syncer = GA4SheetsSync(CONFIG['PROPERTY_ID'], CONFIG['SHEET_ID'])
        
        # Perform the sync
        syncer.sync_data(
            start_date=CONFIG['START_DATE'],
            end_date=CONFIG['END_DATE'],
            dimensions=CONFIG['DIMENSIONS'],
            metrics=CONFIG['METRICS'],
            worksheet_name=CONFIG['WORKSHEET_NAME']
        )
        
    except Exception as e:
        logger.error(f"❌ Application failed: {e}")
        import traceback
        traceback.print_exc()

# For continuous sync (optional)hh
def continuous_sync(interval_minutes: int = 60):
    """Run sync continuously at specified intervals"""
    logger.info(f"🔄 Starting continuous sync every {interval_minutes} minutes")
    
    while True:
        try:
            main()
            logger.info(f"😴 Waiting {interval_minutes} minutes for next sync...")
            time.sleep(interval_minutes * 60)
        except KeyboardInterrupt:
            logger.info("⏹️ Continuous sync stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Error in continuous sync: {e}")
            logger.info(f"⏳ Waiting {interval_minutes} minutes before retry...")
            time.sleep(interval_minutes * 60)

if __name__ == "__main__":
    # For one-time sync
    main()
    
    # Uncomment below for continuous sync every hour
    continuous_sync(60)