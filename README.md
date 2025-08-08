# PySpark Transaction Processor

A high-performance transaction processing system that ingests data from Google Drive and detects customer upgrade patterns using PySpark. Designed to handle 10,000+ transactions per second with real-time pattern detection capabilities.

## 🏗️ System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Google Drive  │───▶│   Mechanism X   │───▶│      AWS S3     │
│  (CSV Files)    │    │  (Data Ingest)  │    │ (Raw Storage)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │◀───│   Mechanism Y   │◀───│   PySpark       │
│   (Results)     │    │ (Pattern Detect)│    │  (Processing)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📊 Data Flow Overview

### **Phase 1: Data Ingestion (Mechanism X)**
1. **Google Drive Monitoring**: Continuously monitors specified folder for new CSV files
2. **File Processing**: Downloads and validates transaction files (10K+ records/sec capability)
3. **S3 Upload**: Streams processed data to AWS S3 with partitioned structure
4. **Metadata Tracking**: Records file processing status in PostgreSQL

### **Phase 2: Pattern Detection (Mechanism Y)**
1. **S3 Data Reading**: PySpark reads partitioned data from S3 buckets
2. **Data Transformation**: Applies schema validation and data cleaning
3. **Pattern Analysis**: Uses Spark Window functions for percentile calculations
4. **Result Storage**: Saves detected patterns to PostgreSQL and S3

## 🎯 Pattern Detection Logic

### **PatId1 - Customer Upgrade Detection**
Identifies potential upgrade candidates using advanced analytics:

```sql
-- Customers in TOP 10% by transaction count
-- AND BOTTOM 10% by importance weight
-- For merchants with >50K total transactions
```

**Algorithm Steps:**
1. **Merchant Filtering**: Filter merchants with >50,000 transactions
2. **Percentile Calculation**: Calculate 90th percentile for transaction counts
3. **Importance Scoring**: Calculate 10th percentile for customer importance
4. **Pattern Matching**: Identify customers meeting both criteria
5. **Result Validation**: Verify pattern accuracy and store results

## 🚀 Quick Start

### Prerequisites
- Python 3.8+ with pip
- Java 8 or 11 (for PySpark)
- PostgreSQL 12+
- AWS S3 access credentials
- Google Drive API service account

### Installation

1. **Clone and Setup Environment**:
```bash
git clone <repository-url>
cd PySpark-Transaction-Processor
pip install -r requirements.txt
```

2. **Configure Credentials**:
```bash
cp .env.template .env
# Edit .env with your actual credentials
```

3. **Database Setup**:
```bash
# Create PostgreSQL database
createdb transaction_db
psql transaction_db < setup/postgres_setup.sql
```

4. **Google Drive API Setup**:
   - Create service account in [Google Cloud Console](https://console.cloud.google.com)
   - Download JSON credentials to `config/google_credentials.json`
   - Share target Drive folder with service account email
   - Copy folder ID from Drive URL

5. **AWS S3 Configuration**:
   - Create S3 bucket for data storage
   - Configure IAM user with S3 read/write permissions
   - Add credentials to `.env` file

### Running the System

```bash
# Start the complete pipeline
python main.py

# Run individual mechanisms
python -m src.mechanism_x.data_ingestion  # Data ingestion only
python -m src.mechanism_y.stream_processor # Pattern detection only
```

## 📁 Project Structure

```
├── main.py                           # Main application entry point
├── requirements.txt                  # Python dependencies
├── .env.template                     # Environment configuration template
├── .github/                          # CI/CD and GitHub configurations
│   ├── workflows/ci.yml             # Automated testing pipeline
│   ├── SECURITY.md                  # Security policy
│   └── dependabot.yml               # Dependency updates
├── config/
│   ├── config.py                    # Application configuration
│   ├── spark_config.py              # PySpark optimization settings
│   └── google_credentials.json      # Google Drive API credentials
├── src/
│   ├── mechanism_x/                 # Data Ingestion Pipeline
│   │   ├── data_ingestion.py       # Google Drive to S3 processor
│   │   └── file_monitor.py         # File change detection
│   ├── mechanism_y/                 # Pattern Detection Engine
│   │   ├── stream_processor.py     # Main processing logic
│   │   ├── pattern_detector.py     # PatId1 implementation
│   │   └── data_validator.py       # Data quality checks
│   ├── schemas/                     # Data schemas and validation
│   │   └── transaction_schemas.py  # Spark schema definitions
│   └── utils/                       # Shared utilities
│       ├── postgres_utils.py       # Database operations
│       ├── s3_utils.py             # AWS S3 operations
│       └── spark_utils.py          # Spark session management
├── setup/                           # Database setup scripts
│   ├── postgres_setup.sql          # Initial database schema
│   └── create_missing_tables.sql   # Additional table creation
└── tests/                           # Test suite
    ├── test_mechanism_x.py         # Ingestion tests
    ├── test_mechanism_y.py         # Pattern detection tests
    └── test_integration.py         # End-to-end tests
```

## ⚙️ Configuration

### Environment Variables (.env)
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=your-transaction-bucket

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=transaction_db
POSTGRES_USER=transaction_user
POSTGRES_PASSWORD=your_password

# Google Drive Configuration
GDRIVE_FOLDER_ID=your_folder_id
GOOGLE_CREDENTIALS_PATH=config/google_credentials.json

# Processing Configuration
BATCH_SIZE=10000
MAX_WORKERS=4
PROCESSING_INTERVAL=30
```

### Spark Configuration
- **Memory**: 4GB driver, 2GB executor
- **Cores**: Auto-detected based on system
- **Serializer**: Kryo for performance
- **Checkpointing**: Enabled for fault tolerance

## 📈 Performance Metrics

### **Throughput Capabilities**
- **Data Ingestion**: 10,000+ transactions/second
- **Pattern Detection**: 1M+ records processed in <5 minutes
- **S3 Upload**: Parallel multipart uploads for large files
- **Database Writes**: Batch inserts with connection pooling

### **System Requirements**
- **Minimum**: 4GB RAM, 2 CPU cores
- **Recommended**: 8GB RAM, 4 CPU cores
- **Storage**: 10GB+ free space for local processing

## 📊 Output Data Structure

### **S3 Data Organization**
```
s3://your-bucket/
├── raw-transactions/
│   ├── year=2024/month=01/day=15/
│   │   ├── transactions_001.parquet
│   │   └── transactions_002.parquet
│   └── _metadata/
│       └── processing_log.json
└── pattern-detections/
    ├── patid1/
    │   ├── year=2024/month=01/
    │   │   └── detections.parquet
    │   └── summary/
    │       └── monthly_stats.json
    └── validation/
        └── quality_reports.json
```

### **PostgreSQL Tables**
- **`transactions`**: Raw transaction data with indexing
- **`customer_importance`**: Customer scoring and weights
- **`pattern_detections`**: PatId1 results with timestamps
- **`processing_logs`**: System monitoring and audit trail

## 🔍 Monitoring & Observability

### **Logging**
- Structured JSON logging with correlation IDs
- Separate log levels for development and production
- Automatic log rotation and archival

### **Metrics**
- Processing throughput and latency tracking
- Error rates and system health monitoring
- Resource utilization (CPU, memory, disk)

### **Alerts**
- Failed file processing notifications
- Database connection issues
- S3 upload failures and retries

## 🛠️ Development

### **Testing**
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=src tests/

# Run specific test categories
pytest tests/test_mechanism_x.py -v
```

### **Code Quality**
- **Linting**: Flake8 for PEP 8 compliance
- **Formatting**: Black for consistent code style
- **Security**: Bandit for security vulnerability scanning
- **Dependencies**: Safety for known security issues

### **CI/CD Pipeline**
- Automated testing on Python 3.8, 3.9, 3.10
- Security scanning and dependency checks
- Code coverage reporting
- Automated deployment to staging environment

## 🔒 Security

- **Credentials**: Environment-based configuration
- **API Keys**: Encrypted storage and rotation
- **Database**: Connection encryption and user isolation
- **S3**: IAM-based access control with minimal permissions
- **Audit**: Complete processing trail and access logging

## 📞 Support

For technical issues or questions:
1. Check the [troubleshooting guide](TROUBLESHOOTING.md)
2. Review [GitHub Issues](../../issues)
3. Contact: hiring@devdolphins.com

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.