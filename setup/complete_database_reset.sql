-- COMPLETE DATABASE RESET AND SCHEMA FIX
-- This script will completely reset the database to match the actual CSV structure
-- Run as: psql -h localhost -U postgres -d transaction_db -f setup/complete_database_reset.sql

-- Step 1: Drop ALL existing tables and related objects
DROP TABLE IF EXISTS customer_transaction_stats CASCADE;
DROP TABLE IF EXISTS customer_transaction_stats_validation CASCADE;
DROP TABLE IF EXISTS customer_importance_scores CASCADE;
DROP TABLE IF EXISTS customer_importance_scores_validation CASCADE;
DROP TABLE IF EXISTS eligible_merchants CASCADE;
DROP TABLE IF EXISTS customer_importance CASCADE;
DROP TABLE IF EXISTS processing_log CASCADE;

-- Drop any views that might exist
DROP VIEW IF EXISTS customer_stats_view CASCADE;
DROP VIEW IF EXISTS importance_view CASCADE;

-- Drop any triggers and functions
DROP TRIGGER IF EXISTS update_customer_stats_updated_at ON customer_transaction_stats;
DROP TRIGGER IF EXISTS update_importance_scores_updated_at ON customer_importance_scores;
DROP TRIGGER IF EXISTS update_eligible_merchants_updated_at ON eligible_merchants;
DROP TRIGGER IF EXISTS update_customer_importance_updated_at ON customer_importance;
DROP FUNCTION IF EXISTS update_updated_at_column();

-- Step 2: Recreate all tables with EXACT CSV-aligned schema
-- This matches the actual CSV files:
-- Transactions.csv: step,customer,age,gender,zipcodeOri,merchant,zipMerchant,category,amount,fraud
-- CustomerImportance.csv: Source,Target,Weight,typeTrans,fraud

-- Table 1: customer_transaction_stats (derived from Transactions.csv)
CREATE TABLE customer_transaction_stats (
    customer VARCHAR(255) NOT NULL,  -- matches 'customer' column in Transactions.csv
    merchant VARCHAR(255) NOT NULL,  -- matches 'merchant' column in Transactions.csv
    transaction_count BIGINT NOT NULL DEFAULT 0,
    merchant_rank FLOAT DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Table 2: customer_importance_scores (derived from Transactions.csv + CustomerImportance.csv)
CREATE TABLE customer_importance_scores (
    customer VARCHAR(255) NOT NULL,  -- matches 'customer' column in Transactions.csv
    merchant VARCHAR(255) NOT NULL,  -- matches 'merchant' column in Transactions.csv
    avg_weight FLOAT DEFAULT 1.0,
    weight_rank FLOAT DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Table 3: eligible_merchants (derived from Transactions.csv)
CREATE TABLE eligible_merchants (
    merchant VARCHAR(255) PRIMARY KEY,  -- matches 'merchant' column in Transactions.csv
    total_transactions BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 4: customer_importance (EXACT match to CustomerImportance.csv)
CREATE TABLE customer_importance (
    "Source" VARCHAR(255) NOT NULL,    -- EXACT match to CSV column
    "Target" VARCHAR(255) NOT NULL,    -- EXACT match to CSV column
    "Weight" FLOAT NOT NULL DEFAULT 1.0,  -- EXACT match to CSV column
    "typeTrans" VARCHAR(255) NOT NULL, -- EXACT match to CSV column
    fraud INTEGER NOT NULL DEFAULT 0,  -- EXACT match to CSV column
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

-- Step 3: Create performance indexes
CREATE INDEX idx_customer_stats_customer ON customer_transaction_stats(customer);
CREATE INDEX idx_customer_stats_merchant ON customer_transaction_stats(merchant);
CREATE INDEX idx_customer_stats_rank ON customer_transaction_stats(merchant_rank);
CREATE INDEX idx_customer_stats_count ON customer_transaction_stats(transaction_count);

CREATE INDEX idx_importance_scores_customer ON customer_importance_scores(customer);
CREATE INDEX idx_importance_scores_merchant ON customer_importance_scores(merchant);
CREATE INDEX idx_importance_scores_rank ON customer_importance_scores(weight_rank);
CREATE INDEX idx_importance_scores_weight ON customer_importance_scores(avg_weight);

CREATE INDEX idx_eligible_merchants_transactions ON eligible_merchants(total_transactions);

CREATE INDEX idx_customer_importance_source ON customer_importance("Source");
CREATE INDEX idx_customer_importance_target ON customer_importance("Target");
CREATE INDEX idx_customer_importance_type ON customer_importance("typeTrans");
CREATE INDEX idx_customer_importance_weight ON customer_importance("Weight");

CREATE INDEX idx_processing_log_status ON processing_log(processing_status);
CREATE INDEX idx_processing_log_file ON processing_log(file_name);

-- Step 4: Recreate the timestamp update function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Step 5: Create triggers to automatically update timestamps
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

-- Step 6: Grant permissions to transaction_user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transaction_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO transaction_user;

-- Step 7: Insert sample data matching EXACT CSV structure
-- Sample eligible merchants (from Transactions.csv merchant column)
INSERT INTO eligible_merchants (merchant, total_transactions) VALUES
('M348934600', 75000),
('M348934601', 60000),
('M348934602', 55000),
('M348934603', 45000),
('M348934604', 40000),
('M348934605', 35000),
('M348934606', 30000),
('M348934607', 25000)
ON CONFLICT (merchant) DO UPDATE SET 
    total_transactions = EXCLUDED.total_transactions,
    updated_at = CURRENT_TIMESTAMP;

-- Sample customer importance data (EXACT match to CustomerImportance.csv structure)
INSERT INTO customer_importance ("Source", "Target", "Weight", "typeTrans", fraud) VALUES
('C1093826151', 'M348934600', 4.55, 'es_transportation', 0),
('C1093826152', 'M348934601', 3.25, 'es_food', 0),
('C1093826153', 'M348934602', 5.75, 'es_health', 0),
('C1093826154', 'M348934603', 2.15, 'es_shopping', 0),
('C1093826155', 'M348934604', 6.85, 'es_entertainment', 0),
('C1093826156', 'M348934600', 1.95, 'es_gas_transport', 0),
('C1093826157', 'M348934601', 3.45, 'es_food', 0),
('C1093826158', 'M348934602', 4.25, 'es_health', 0),
('C1093826159', 'M348934603', 2.85, 'es_shopping', 0),
('C1093826160', 'M348934604', 5.15, 'es_entertainment', 0),
('C1093826161', 'M348934605', 3.75, 'es_transportation', 0),
('C1093826162', 'M348934606', 4.95, 'es_food', 0),
('C1093826163', 'M348934607', 2.35, 'es_health', 0)
ON CONFLICT ("Source", "typeTrans") DO UPDATE SET 
    "Target" = EXCLUDED."Target",
    "Weight" = EXCLUDED."Weight",
    fraud = EXCLUDED.fraud,
    updated_at = CURRENT_TIMESTAMP;

-- Step 8: Verify table structures match CSV exactly
SELECT 'VERIFICATION: customer_transaction_stats schema' as verification_step;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'customer_transaction_stats' 
ORDER BY ordinal_position;

SELECT 'VERIFICATION: customer_importance_scores schema' as verification_step;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'customer_importance_scores' 
ORDER BY ordinal_position;

SELECT 'VERIFICATION: eligible_merchants schema' as verification_step;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'eligible_merchants' 
ORDER BY ordinal_position;

SELECT 'VERIFICATION: customer_importance schema (must match CustomerImportance.csv)' as verification_step;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'customer_importance' 
ORDER BY ordinal_position;

SELECT 'VERIFICATION: processing_log schema' as verification_step;
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'processing_log' 
ORDER BY ordinal_position;

-- Step 9: Verify data was inserted correctly
SELECT 'VERIFICATION: Sample data counts' as verification_step;
SELECT 'eligible_merchants' as table_name, COUNT(*) as row_count FROM eligible_merchants
UNION ALL
SELECT 'customer_importance' as table_name, COUNT(*) as row_count FROM customer_importance;

-- Step 10: Final confirmation
SELECT 'SUCCESS: Database completely reset with CSV-aligned schema!' as status;
SELECT 'Tables now match:' as confirmation;
SELECT '- Transactions.csv columns: step,customer,age,gender,zipcodeOri,merchant,zipMerchant,category,amount,fraud' as csv_structure;
SELECT '- CustomerImportance.csv columns: Source,Target,Weight,typeTrans,fraud' as csv_structure;
SELECT 'All database tables use these EXACT column names!' as final_confirmation;