-- PostgreSQL Setup Script for Transaction Processor
-- Run this script to create the database and initial schema

-- Create database (run as superuser)
-- CREATE DATABASE transaction_db;

-- Connect to the database and create tables
\c transaction_db;

-- Create tables for transaction processing
CREATE TABLE IF NOT EXISTS customer_transaction_stats (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    transaction_count BIGINT,
    merchant_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

CREATE TABLE IF NOT EXISTS customer_importance_scores (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    avg_weight FLOAT,
    weight_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

CREATE TABLE IF NOT EXISTS eligible_merchants (
    merchant VARCHAR(255) PRIMARY KEY,
    total_transactions BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_importance (
    "Source" VARCHAR(255),
    "Target" VARCHAR(255),
    "Weight" FLOAT,
    "typeTrans" VARCHAR(255),
    fraud INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("Source", "typeTrans")
);

CREATE TABLE IF NOT EXISTS processing_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(500),
    processing_status VARCHAR(50),
    detections_found INTEGER,
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_customer_stats_merchant ON customer_transaction_stats(merchant);
CREATE INDEX IF NOT EXISTS idx_customer_stats_rank ON customer_transaction_stats(merchant_rank);
CREATE INDEX IF NOT EXISTS idx_importance_scores_merchant ON customer_importance_scores(merchant);
CREATE INDEX IF NOT EXISTS idx_importance_scores_rank ON customer_importance_scores(weight_rank);
CREATE INDEX IF NOT EXISTS idx_processing_log_status ON processing_log(processing_status);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update timestamps
CREATE TRIGGER update_customer_stats_updated_at 
    BEFORE UPDATE ON customer_transaction_stats 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_importance_scores_updated_at 
    BEFORE UPDATE ON customer_importance_scores 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_eligible_merchants_updated_at 
    BEFORE UPDATE ON eligible_merchants 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant necessary permissions (adjust user as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transaction_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;

SELECT 'Database schema created successfully!' as status;