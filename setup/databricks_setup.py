"""
Databricks setup script for PySpark Transaction Processor
Run this in a Databricks notebook to install required packages
"""

# Install required packages
%pip install boto3 psycopg2-binary google-api-python-client google-auth-httplib2 google-auth-oauthlib python-dotenv structlog pytz

# Restart Python to use new packages
dbutils.library.restartPython()

# Import and test packages
try:
    import boto3
    import psycopg2
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    import structlog
    import pytz
    print("✅ All packages installed successfully!")
except ImportError as e:
    print(f"❌ Package installation failed: {e}")

# Configure Spark for S3 access
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# Set your AWS credentials (replace with actual values or use Databricks secrets)
# spark.conf.set("spark.hadoop.fs.s3a.access.key", dbutils.secrets.get(scope="aws", key="access_key"))
# spark.conf.set("spark.hadoop.fs.s3a.secret.key", dbutils.secrets.get(scope="aws", key="secret_key"))

print("✅ Databricks environment configured!")

# Test S3 connectivity (uncomment and modify with your bucket)
# try:
#     spark.read.text("s3a://your-bucket-name/").show(5)
#     print("✅ S3 connectivity test passed!")
# except Exception as e:
#     print(f"❌ S3 connectivity test failed: {e}")