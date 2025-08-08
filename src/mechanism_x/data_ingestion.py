import time
import threading
from datetime import datetime
from config.config import Config
from src.mechanism_x.gdrive_reader import GDriveReader
from src.utils.s3_utils import S3Utils
from src.utils.logger import setup_logger
import structlog

logger = structlog.get_logger()

class DataIngestionService:
    def __init__(self):
        self.gdrive_reader = None
        self.s3_utils = S3Utils()
        self.running = False
        self.thread = None
        self._initialize_gdrive_reader()
    
    def _initialize_gdrive_reader(self):
        """Initialize Google Drive reader with error handling"""
        try:
            logger.info("Initializing Google Drive reader")
            self.gdrive_reader = GDriveReader()
            logger.info("Google Drive reader initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize Google Drive reader", error=str(e))
            raise
    
    def start(self):
        """Start the data ingestion service"""
        if self.running:
            logger.warning("Data ingestion service is already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_ingestion, daemon=True)
        self.thread.start()
        logger.info("Data ingestion service started")
    
    def stop(self):
        """Stop the data ingestion service with graceful shutdown"""
        self.running = False
        
        # Wait for ingestion thread to finish
        if self.thread and self.thread.is_alive():
            try:
                self.thread.join(timeout=3.0)  # Wait max 3 seconds
                if self.thread.is_alive():
                    logger.warning("Data ingestion thread did not stop gracefully")
            except Exception as e:
                logger.warning("Error joining ingestion thread", error=str(e))
        
        logger.info("Data ingestion service stopped")
    
    def _run_ingestion(self):
        """Main ingestion loop - processes all data once then stops"""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while self.running:
            try:
                start_time = time.time()
                
                # Check if gdrive_reader is available
                if self.gdrive_reader is None:
                    logger.warning("Google Drive reader not available, attempting to reinitialize")
                    self._initialize_gdrive_reader()
                
                # Get next chunk of data
                chunk = self.gdrive_reader.get_next_chunk()
                
                if not chunk.empty:
                    # Generate timestamp-based filename
                    timestamp = datetime.now(Config.IST).strftime("%Y%m%d_%H%M%S_%f")[:-3]
                    filename = f"transactions_chunk_{timestamp}.csv"
                    
                    # Upload to S3
                    s3_key = f"{Config.S3_RAW_DATA_PREFIX}{filename}"
                    self.s3_utils.upload_dataframe(chunk, s3_key)
                    
                    logger.info("Uploaded chunk to S3",
                               filename=filename,
                               rows=len(chunk),
                               s3_key=s3_key)
                    
                    # Reset error counter on success
                    consecutive_errors = 0
                else:
                    # Empty chunk means all data has been processed
                    if self.gdrive_reader and self.gdrive_reader.is_complete():
                        logger.info("✅ DATA INGESTION COMPLETED - All transactions have been processed and uploaded to S3")
                        self.running = False
                        break
                    else:
                        logger.warning("Empty chunk received but processing not complete")
                
                # Ensure we wait at least 1 second between iterations
                elapsed = time.time() - start_time
                sleep_time = max(0, Config.PROCESSING_INTERVAL_SECONDS - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
            except Exception as e:
                consecutive_errors += 1
                logger.error("Error in data ingestion loop",
                           error=str(e),
                           consecutive_errors=consecutive_errors)
                
                # If too many consecutive errors, try to reinitialize
                if consecutive_errors >= max_consecutive_errors:
                    logger.warning("Too many consecutive errors, attempting to reinitialize Google Drive reader")
                    try:
                        self.gdrive_reader = None
                        self._initialize_gdrive_reader()
                        consecutive_errors = 0
                        logger.info("Google Drive reader reinitialized successfully")
                    except Exception as reinit_error:
                        logger.error("Failed to reinitialize Google Drive reader", error=str(reinit_error))
                        # Continue with exponential backoff
                        sleep_time = min(60, Config.PROCESSING_INTERVAL_SECONDS * (2 ** min(consecutive_errors, 6)))
                        logger.info(f"Backing off for {sleep_time} seconds")
                        time.sleep(sleep_time)
                        continue
                
                time.sleep(Config.PROCESSING_INTERVAL_SECONDS)
        
        logger.info("Data ingestion service completed successfully")