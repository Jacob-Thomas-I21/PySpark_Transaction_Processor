#!/bin/bash

# PySpark Transaction Processor Setup Script
echo "=========================================="
echo "PySpark Transaction Processor Setup"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if .env file exists
if [ ! -f ".env" ]; then
    print_warning ".env file not found. Copying from template..."
    cp .env.template .env
    print_warning "Please edit .env file with your actual configuration values"
    exit 1
fi

print_status "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

print_status "Installing Python dependencies..."
pip install -r requirements.txt

print_status "Setting up directory structure..."
mkdir -p logs
mkdir -p data/temp
mkdir -p output

# Check AWS CLI configuration
print_status "Checking AWS configuration..."
if ! aws sts get-caller-identity > /dev/null 2>&1; then
    print_warning "AWS CLI not configured. Please run 'aws configure'"
fi

# Create S3 bucket if it doesn't exist
print_status "Checking S3 bucket..."
BUCKET_NAME=$(grep S3_BUCKET_NAME .env | cut -d '=' -f2)
if [ ! -z "$BUCKET_NAME" ]; then
    aws s3 mb s3://$BUCKET_NAME 2>/dev/null || print_warning "Bucket might already exist or access denied"
    
    # Create folder structure
    aws s3api put-object --bucket $BUCKET_NAME --key raw-transactions/ --content-length 0
    aws s3api put-object --bucket $BUCKET_NAME --key pattern-detections/ --content-length 0
fi

print_status "Running database setup..."
python3 -c "
from src.utils.postgres_utils import PostgresUtils
try:
    pg = PostgresUtils()
    pg.create_tables()
    print('Database tables created successfully')
except Exception as e:
    print(f'Database setup failed: {e}')
    print('Please ensure PostgreSQL is accessible and credentials are correct')
"

print_status "Setup completed!"
print_warning "Before running the application:"
print_warning "1. Ensure your .env file has correct values"
print_warning "2. Place your Google service account JSON file in config/"
print_warning "3. Ensure Databricks cluster is running"
print_warning "4. Test database connectivity"

echo ""
echo "To run the application:"
echo "  python main.py"