import os
from dotenv import load_dotenv
import pytz

load_dotenv()

class Config:
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    
    # Google Drive Configuration
    GDRIVE_FOLDER_ID = os.getenv('GDRIVE_FOLDER_ID')
    GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH')
    
    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
    POSTGRES_DB = os.getenv('POSTGRES_DB')
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    
    # Databricks Configuration
    DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
    DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
    DATABRICKS_CLUSTER_ID = os.getenv('DATABRICKS_CLUSTER_ID')
    
    # Processing Configuration
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 10000))
    DETECTION_BATCH_SIZE = int(os.getenv('DETECTION_BATCH_SIZE', 50))
    PROCESSING_INTERVAL_SECONDS = int(os.getenv('PROCESSING_INTERVAL_SECONDS', 1))
    MERCHANT_THRESHOLD = int(os.getenv('MERCHANT_THRESHOLD', 50000))
    
    # Timezone
    IST = pytz.timezone('Asia/Kolkata')
    
    # S3 Paths
    S3_RAW_DATA_PREFIX = 'raw-transactions/'
    S3_DETECTIONS_PREFIX = 'pattern-detections/'
    
    @classmethod
    def validate(cls):
        """Validate that all required configuration is present"""
        required_vars = [
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET_NAME',
            'POSTGRES_HOST', 'POSTGRES_USER', 'POSTGRES_PASSWORD',
            'DATABRICKS_HOST', 'DATABRICKS_TOKEN'
        ]
        
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")