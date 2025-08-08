-- Comprehensive Database Schema Fix
-- This script will drop ALL existing tables and recreate them with the correct CSV-aligned schema
-- Run as: psql -h localhost -U postgres -d transaction_db -f setup/fix_database_schema.sql

-- Drop ALL existing tables that might have wrong schema (including any variations)
DROP TABLE IF EXISTS customer_transaction_stats CASCADE;
DROP TABLE IF EXISTS customer_transaction_stats_validation CASCADE;
DROP TABLE IF EXISTS customer_importance_scores CASCADE;
DROP TABLE IF EXISTS customer_importance_scores_validation CASCADE;
DROP TABLE IF EXISTS eligible_merchants CASCADE;
DROP TABLE IF EXISTS customer_importance CASCADE;
DROP TABLE IF EXISTS processing_log CASCADE;

-- Drop any triggers and functions that might exist
DROP TRIGGER IF EXISTS update_customer_stats_updated_at ON customer_transaction_stats;
DROP TRIGGER IF EXISTS update_importance_scores_updated_at ON customer_importance_scores;
DROP TRIGGER IF EXISTS update_eligible_merchants_updated_at ON eligible_merchants;
DROP FUNCTION IF EXISTS update_updated_at_column();

-- Recreate all tables with correct CSV-aligned schema
-- Table 1: customer_transaction_stats (matches CSV: customer, merchant)
CREATE TABLE customer_transaction_stats (
    customer VARCHAR(255) NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    transaction_count BIGINT NOT NULL DEFAULT 0,
    merchant_rank FLOAT DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Table 2: customer_importance_scores (matches CSV: customer, merchant)
CREATE TABLE customer_importance_scores (
    customer VARCHAR(255) NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    avg_weight FLOAT DEFAULT 1.0,
    weight_rank FLOAT DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Table 3: eligible_merchants (matches CSV: merchant)
CREATE TABLE eligible_merchants (
    merchant VARCHAR(255) PRIMARY KEY,
    total_transactions BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: customer_importance (matches CustomerImportance.csv: Source,Target,Weight,typeTrans,fraud)
CREATE TABLE customer_importance (
    "Source" VARCHAR(255) NOT NULL,
    "Target" VARCHAR(255) NOT NULL,
    "Weight" FLOAT NOT NULL DEFAULT 1.0,
    "typeTrans" VARCHAR(255) NOT NULL,
    fraud INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("Source", "typeTrans")
);

-- Table 5: processing_log (for system logging)
CREATE TABLE processing_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(500),
    processing_status VARCHAR(50),
    detections_found INTEGER DEFAULT 0,
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create performance indexes
CREATE INDEX IF NOT EXISTS idx_customer_stats_customer ON customer_transaction_stats(customer);
CREATE INDEX IF NOT EXISTS idx_customer_stats_merchant ON customer_transaction_stats(merchant);
CREATE INDEX IF NOT EXISTS idx_customer_stats_rank ON customer_transaction_stats(merchant_rank);
CREATE INDEX IF NOT EXISTS idx_customer_stats_count ON customer_transaction_stats(transaction_count);

CREATE INDEX IF NOT EXISTS idx_importance_scores_customer ON customer_importance_scores(customer);
CREATE INDEX IF NOT EXISTS idx_importance_scores_merchant ON customer_importance_scores(merchant);
CREATE INDEX IF NOT EXISTS idx_importance_scores_rank ON customer_importance_scores(weight_rank);
CREATE INDEX IF NOT EXISTS idx_importance_scores_weight ON customer_importance_scores(avg_weight);

CREATE INDEX IF NOT EXISTS idx_eligible_merchants_transactions ON eligible_merchants(total_transactions);

CREATE INDEX IF NOT EXISTS idx_customer_importance_source ON customer_importance("Source");
CREATE INDEX IF NOT EXISTS idx_customer_importance_target ON customer_importance("Target");
CREATE INDEX IF NOT EXISTS idx_customer_importance_type ON customer_importance("typeTrans");
CREATE INDEX IF NOT EXISTS idx_customer_importance_weight ON customer_importance("Weight");

CREATE INDEX IF NOT EXISTS idx_processing_log_status ON processing_log(processing_status);
CREATE INDEX IF NOT EXISTS idx_processing_log_file ON processing_log(file_name);

-- Recreate the timestamp update function
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

CREATE TRIGGER update_customer_importance_updated_at 
    BEFORE UPDATE ON customer_importance 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions to transaction_user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transaction_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO transaction_user;

-- Insert sample data matching actual CSV structure
-- Sample eligible merchants (merchant column from Transactions.csv)
INSERT INTO eligible_merchants (merchant, total_transactions) VALUES
('M348934600', 75000),
('M348934601', 60000),
('M348934602', 55000),
('M348934603', 45000),
('M348934604', 40000)
ON CONFLICT (merchant) DO UPDATE SET 
    total_transactions = EXCLUDED.total_transactions,
    updated_at = CURRENT_TIMESTAMP;

-- Sample customer importance data (Source,Target,Weight,typeTrans,fraud from CustomerImportance.csv)
INSERT INTO customer_importance ("Source", "Target", "Weight", "typeTrans", fraud) VALUES
('C1093826151', 'M348934600', 4.55, 'es_transportation', 0),
('C1093826152', 'M348934601', 3.25, 'es_food', 0),
('C1093826153', 'M348934602', 5.75, 'es_health', 0),
('C1093826154', 'M348934603', 2.15, 'es_shopping', 0),
('C1093826155', 'M348934604', 6.85, 'es_entertainment', 0),
('C1093826156', 'M348934600', 1.95, 'es_gas_transport', 0),
('C1093826157', 'M348934601', 3.45, 'es_food', 0),
('C1093826158', 'M348934602', 4.25, 'es_health', 0)
ON CONFLICT ("Source", "typeTrans") DO UPDATE SET 
    "Target" = EXCLUDED."Target",
    "Weight" = EXCLUDED."Weight",
    fraud = EXCLUDED.fraud,
    updated_at = CURRENT_TIMESTAMP;

-- Verify table structures
SELECT 'customer_transaction_stats' as table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'customer_transaction_stats' 
ORDER BY ordinal_position;

SELECT 'customer_importance_scores' as table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'customer_importance_scores' 
ORDER BY ordinal_position;

SELECT 'eligible_merchants' as table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'eligible_merchants' 
ORDER BY ordinal_position;

SELECT 'customer_importance' as table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'customer_importance' 
ORDER BY ordinal_position;

SELECT 'processing_log' as table_name, column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'processing_log' 
ORDER BY ordinal_position;

-- Final status
SELECT 'All database tables recreated with correct CSV-aligned schema!' as status;
SELECT 'Tables now match: Transactions.csv (customer,merchant) and CustomerImportance.csv (Source,Target,Weight,typeTrans,fraud)' as confirmation;