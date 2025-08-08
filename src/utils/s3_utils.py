import boto3
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
from config.config import Config
import structlog

logger = structlog.get_logger()

class S3Utils:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
            region_name=Config.AWS_REGION
        )
        self.bucket_name = Config.S3_BUCKET_NAME
    
    def upload_dataframe(self, df, s3_key):
        """Upload pandas DataFrame as CSV to S3"""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            logger.debug("DataFrame uploaded to S3", s3_key=s3_key, rows=len(df))
            
        except ClientError as e:
            logger.error("Failed to upload to S3", s3_key=s3_key, error=str(e))
            raise
    
    def list_files(self, prefix=""):
        """List files in S3 bucket with given prefix"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
            
            return files
            
        except ClientError as e:
            logger.error("Failed to list S3 files", prefix=prefix, error=str(e))
            return []
    
    def download_file(self, s3_key, local_path):
        """Download file from S3 to local path"""
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.debug("File downloaded from S3", s3_key=s3_key, local_path=local_path)
        except ClientError as e:
            logger.error("Failed to download from S3", s3_key=s3_key, error=str(e))
            raise
    
    def file_exists(self, s3_key):
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False