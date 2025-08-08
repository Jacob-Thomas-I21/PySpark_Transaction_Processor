# Troubleshooting Guide

This guide covers common issues and solutions for the PySpark Transaction Processor.

## Installation Issues

### Python Dependencies

**Problem**: Package installation fails with dependency conflicts
```
ERROR: pip's dependency resolver does not currently consider all the packages that are installed
```

**Solution**:
```bash
# Create fresh virtual environment
python -m venv myenv
source myenv/bin/activate  # On Windows: myenv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

### Java/PySpark Setup

**Problem**: PySpark fails to start with Java errors
```
Exception: Java gateway process exited before sending its port number
```

**Solutions**:
1. Install Java 8 or 11 (not newer versions)
2. Set JAVA_HOME environment variable
3. On Windows, ensure no spaces in Java path

```bash
# Check Java version
java -version

# Set JAVA_HOME (Windows)
set JAVA_HOME=C:\Program Files\Java\jdk-11.0.x

# Set JAVA_HOME (Linux/Mac)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

## Database Connection Issues

### PostgreSQL Connection Failed

**Problem**: Cannot connect to PostgreSQL database
```
psycopg2.OperationalError: could not connect to server
```

**Solutions**:
1. Verify PostgreSQL is running
2. Check connection parameters in .env file
3. Ensure database and user exist

```bash
# Check if PostgreSQL is running
sudo systemctl status postgresql  # Linux
brew services list | grep postgres  # Mac
sc query postgresql-x64-14  # Windows

# Create database and user
createdb transaction_db
createuser -P transaction_user
```

### Permission Denied Errors

**Problem**: Database permission errors
```
permission denied for table transactions
```

**Solution**:
```sql
-- Connect as postgres superuser
psql -U postgres transaction_db

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE transaction_db TO transaction_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transaction_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;
```

## AWS S3 Issues

### Authentication Errors

**Problem**: S3 access denied or credential errors
```
botocore.exceptions.NoCredentialsError: Unable to locate credentials
```

**Solutions**:
1. Verify AWS credentials in .env file
2. Check IAM user permissions
3. Test credentials with AWS CLI

```bash
# Test AWS credentials
aws s3 ls s3://your-bucket-name

# Configure AWS CLI (alternative)
aws configure
```

### Bucket Access Issues

**Problem**: S3 bucket not found or access denied
```
botocore.exceptions.ClientError: The specified bucket does not exist
```

**Solutions**:
1. Verify bucket name in .env file
2. Check bucket region settings
3. Ensure IAM user has S3 permissions

Required IAM permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket-name",
                "arn:aws:s3:::your-bucket-name/*"
            ]
        }
    ]
}
```

## Google Drive API Issues

### Authentication Failures

**Problem**: Google Drive API authentication errors
```
google.auth.exceptions.DefaultCredentialsError: File not found
```

**Solutions**:
1. Download service account JSON file
2. Place in config/google_credentials.json
3. Share Drive folder with service account email

### File Access Permissions

**Problem**: Cannot access files in Google Drive folder
```
HttpError 403: Insufficient Permission
```

**Solutions**:
1. Share folder with service account email
2. Grant "Editor" or "Viewer" permissions
3. Verify folder ID in .env file

## Runtime Errors

### Memory Issues

**Problem**: Out of memory errors during processing
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions**:
1. Increase Spark driver memory
2. Reduce batch size
3. Enable Spark dynamic allocation

```python
# In spark_config.py
spark_conf = SparkConf()
spark_conf.set("spark.driver.memory", "4g")
spark_conf.set("spark.executor.memory", "2g")
spark_conf.set("spark.sql.adaptive.enabled", "true")
```

### File Processing Errors

**Problem**: CSV parsing errors
```
pyspark.sql.utils.AnalysisException: CSV header does not conform to the schema
```

**Solutions**:
1. Verify CSV file format matches schema
2. Check for encoding issues (use UTF-8)
3. Handle malformed records

```python
# Read CSV with error handling
df = spark.read.option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .csv("path/to/file.csv", header=True, schema=schema)
```

## Performance Issues

### Slow Processing

**Problem**: Data processing takes too long

**Solutions**:
1. Increase parallelism
2. Optimize Spark configuration
3. Use appropriate file formats (Parquet)
4. Partition data properly

```python
# Optimize Spark settings
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### High Memory Usage

**Problem**: System runs out of memory

**Solutions**:
1. Process data in smaller batches
2. Use DataFrame operations instead of RDDs
3. Cache only necessary data
4. Unpersist unused DataFrames

```python
# Efficient memory usage
df.cache()  # Only cache if reused multiple times
df.unpersist()  # Clean up when done
```

## Network Issues

### Connection Timeouts

**Problem**: Network timeouts during file transfers
```
requests.exceptions.ConnectTimeout: HTTPSConnectionPool
```

**Solutions**:
1. Increase timeout values
2. Check network connectivity
3. Use retry mechanisms
4. Verify firewall settings

### SSL Certificate Errors

**Problem**: SSL verification failures
```
requests.exceptions.SSLError: certificate verify failed
```

**Solutions**:
1. Update certificates
2. Check system time
3. Use proper SSL context (not recommended to disable SSL)

## Logging and Debugging

### Enable Debug Logging

Add to your configuration:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Check Log Files

Default log locations:
- Application logs: `logs/application.log`
- Spark logs: `logs/spark/`
- System logs: `/var/log/` (Linux) or Event Viewer (Windows)

### Common Log Patterns

Look for these patterns in logs:
- `ERROR`: Critical failures
- `WARN`: Potential issues
- `Connection refused`: Network/service issues
- `Permission denied`: Access control problems

## Getting Help

If you continue to experience issues:

1. Check the error message carefully
2. Search for similar issues in documentation
3. Verify all configuration settings
4. Test individual components separately
5. Contact support with detailed error logs

## Environment Validation

Use this checklist to verify your setup:

- [ ] Python 3.8+ installed
- [ ] Java 8 or 11 installed and JAVA_HOME set
- [ ] PostgreSQL running and accessible
- [ ] AWS credentials configured and tested
- [ ] Google Drive API credentials in place
- [ ] All required Python packages installed
- [ ] Environment variables set in .env file
- [ ] Database tables created successfully
- [ ] Network connectivity to all services