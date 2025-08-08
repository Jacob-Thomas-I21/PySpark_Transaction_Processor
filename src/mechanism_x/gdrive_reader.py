import io
import pandas as pd
import time
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError, TransportError
from config.config import Config
import structlog

logger = structlog.get_logger()

class GDriveReader:
    def __init__(self):
        self.service = None
        self.current_position = 0
        self.transactions_df = None
        self.customer_importance_df = None
        self._authenticate_with_retry()
        self._load_data()
    
    def _authenticate_with_retry(self, max_retries=3):
        """Authenticate with Google Drive API with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting Google Drive authentication (attempt {attempt + 1}/{max_retries})")
                
                credentials = Credentials.from_service_account_file(
                    Config.GOOGLE_CREDENTIALS_PATH,
                    scopes=['https://www.googleapis.com/auth/drive.readonly']
                )
                
                # Build service with explicit cache_discovery=False to avoid oauth2client issues
                self.service = build(
                    'drive',
                    'v3',
                    credentials=credentials,
                    cache_discovery=False  # This prevents oauth2client cache issues
                )
                
                # Test the connection with a simple API call
                self.service.about().get(fields="user").execute()
                logger.info("Google Drive authentication successful")
                return
                
            except (RefreshError, TransportError, HttpError) as e:
                logger.warning(f"Authentication attempt {attempt + 1} failed", error=str(e))
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error("All authentication attempts failed", error=str(e))
                    raise
            except Exception as e:
                logger.error("Unexpected error during authentication", error=str(e))
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    raise
    
    def _load_data(self):
        """Load both CSV files from Google Drive with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.info(f"Loading data from Google Drive (attempt {attempt + 1}/{max_retries})")
                
                # Get files from the folder
                results = self.service.files().list(
                    q=f"'{Config.GDRIVE_FOLDER_ID}' in parents",
                    fields="files(id, name)"
                ).execute()
                
                files = results.get('files', [])
                logger.info(f"Found {len(files)} files in Google Drive folder")
                
                for file in files:
                    logger.debug(f"Processing file: {file['name']}")
                    if 'transactions.csv' in file['name'].lower():
                        self.transactions_df = self._download_csv_with_retry(file['id'], file['name'])
                        logger.info("Loaded transactions.csv", rows=len(self.transactions_df))
                    elif 'customerimportance.csv' in file['name'].lower():
                        self.customer_importance_df = self._download_csv_with_retry(file['id'], file['name'])
                        logger.info("Loaded CustomerImportance.csv", rows=len(self.customer_importance_df))
                
                if self.transactions_df is None:
                    raise ValueError("transactions.csv not found in Google Drive folder")
                if self.customer_importance_df is None:
                    raise ValueError("CustomerImportance.csv not found in Google Drive folder")
                
                logger.info("Successfully loaded all data from Google Drive")
                return
                
            except (HttpError, TransportError) as e:
                logger.warning(f"Data loading attempt {attempt + 1} failed", error=str(e))
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    # Re-authenticate if needed
                    self._authenticate_with_retry()
                else:
                    logger.error("All data loading attempts failed", error=str(e))
                    raise
            except Exception as e:
                logger.error("Unexpected error loading data from Google Drive", error=str(e))
                raise
    
    def _download_csv_with_retry(self, file_id, filename, max_retries=3):
        """Download and parse CSV file from Google Drive with retry logic"""
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading {filename} (attempt {attempt + 1}/{max_retries})")
                
                request = self.service.files().get_media(fileId=file_id)
                file_buffer = io.BytesIO()
                downloader = MediaIoBaseDownload(file_buffer, request)
                
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Download progress: {int(status.progress() * 100)}%")
                
                file_buffer.seek(0)
                df = pd.read_csv(file_buffer)
                logger.info(f"Successfully downloaded {filename}", size_mb=file_buffer.tell() / (1024*1024))
                return df
                
            except (HttpError, TransportError) as e:
                logger.warning(f"Download attempt {attempt + 1} failed for {filename}", error=str(e))
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"Retrying download in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All download attempts failed for {filename}", error=str(e))
                    raise
            except Exception as e:
                logger.error(f"Unexpected error downloading {filename}", error=str(e))
                raise
    
    def get_next_chunk(self, chunk_size=Config.CHUNK_SIZE):
        """Get next chunk of transactions - stops when all data is processed"""
        if self.current_position >= len(self.transactions_df):
            # All data has been processed - return empty DataFrame to signal completion
            logger.info("All transactions have been processed",
                       total_rows=len(self.transactions_df),
                       total_chunks=self.current_position // chunk_size)
            return pd.DataFrame()  # Empty DataFrame signals completion
        
        end_position = min(self.current_position + chunk_size, len(self.transactions_df))
        chunk = self.transactions_df.iloc[self.current_position:end_position].copy()
        
        chunk_number = (self.current_position // chunk_size) + 1
        total_chunks = (len(self.transactions_df) + chunk_size - 1) // chunk_size  # Ceiling division
        
        self.current_position = end_position
        
        logger.info("Retrieved chunk",
                   chunk_number=chunk_number,
                   total_chunks=total_chunks,
                   start=self.current_position - len(chunk),
                   end=self.current_position,
                   size=len(chunk),
                   progress_pct=round((self.current_position / len(self.transactions_df)) * 100, 1))
        
        return chunk
    
    def is_complete(self):
        """Check if all data has been processed"""
        return self.current_position >= len(self.transactions_df)
    
    def get_customer_importance(self):
        """Get customer importance data"""
        return self.customer_importance_df.copy()