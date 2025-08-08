-- Fix table ownership and permissions for Spark JDBC operations
-- Run this as postgres superuser: psql -h localhost -U postgres -d transaction_db -f setup/fix_table_ownership_permissions.sql

-- First, ensure transaction_user exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'transaction_user') THEN
        CREATE USER transaction_user WITH PASSWORD 'secure_password';
    END IF;
END
$$;

-- Grant database-level permissions
GRANT CONNECT ON DATABASE transaction_db TO transaction_user;
GRANT CREATE ON SCHEMA public TO transaction_user;
GRANT USAGE ON SCHEMA public TO transaction_user;

-- Drop existing tables if they exist (to recreate with proper ownership)
DROP TABLE IF EXISTS customer_transaction_stats CASCADE;
DROP TABLE IF EXISTS customer_importance_scores CASCADE;
DROP TABLE IF EXISTS eligible_merchants CASCADE;
DROP TABLE IF EXISTS customer_importance CASCADE;
DROP TABLE IF EXISTS processing_log CASCADE;

-- Create tables as transaction_user (so they own them)
SET ROLE transaction_user;

-- Create customer_transaction_stats table
CREATE TABLE customer_transaction_stats (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    transaction_count BIGINT,
    merchant_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Create customer_importance_scores table
CREATE TABLE customer_importance_scores (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    avg_weight FLOAT,
    weight_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Create eligible_merchants table
CREATE TABLE eligible_merchants (
    merchant VARCHAR(255) PRIMARY KEY,
    total_transactions BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customer_importance table
CREATE TABLE customer_importance (
    "Source" VARCHAR(255),
    "Target" VARCHAR(255),
    "Weight" FLOAT,
    "typeTrans" VARCHAR(255),
    fraud INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("Source", "typeTrans")
);

-- Create processing_log table
CREATE TABLE processing_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(500),
    processing_status VARCHAR(50),
    detections_found INTEGER,
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create pattern_detections table for storing detection results
CREATE TABLE pattern_detections (
    id SERIAL PRIMARY KEY,
    customer VARCHAR(255),
    merchant VARCHAR(255),
    detection_type VARCHAR(100),
    confidence_score FLOAT,
    transaction_amount FLOAT,
    detection_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reset role to postgres for final setup
RESET ROLE;

-- Grant additional permissions to transaction_user for sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;

-- Set default privileges for future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO transaction_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO transaction_user;

-- Insert sample data
INSERT INTO eligible_merchants (merchant, total_transactions) VALUES
('M348934600', 75000),
('M348934601', 60000),
('M348934602', 55000)
ON CONFLICT (merchant) DO NOTHING;

INSERT INTO customer_importance ("Source", "Target", "Weight", "typeTrans", fraud) VALUES
('C1093826151', 'M348934600', 4.55, 'es_transportation', 0),
('C1093826152', 'M348934601', 3.25, 'es_food', 0),
('C1093826153', 'M348934602', 5.75, 'es_health', 0),
('C1093826154', 'M348934603', 2.15, 'es_shopping', 0),
('C1093826155', 'M348934604', 6.85, 'es_entertainment', 0)
ON CONFLICT ("Source", "typeTrans") DO NOTHING;

-- Verify table ownership
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'public'
AND tablename IN ('customer_transaction_stats', 'customer_importance_scores', 'eligible_merchants', 'customer_importance', 'processing_log', 'pattern_detections')
ORDER BY tablename;

SELECT 'Tables created with proper ownership for transaction_user!' as status;