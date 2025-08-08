-- Create missing tables as superuser with proper permissions
-- Run this as: psql -h localhost -U postgres -d transaction_db

-- First, become the database owner or create our own schema
-- Check current database owner and public schema permissions
SELECT d.datname, pg_catalog.pg_get_userbyid(d.datdba) as owner 
FROM pg_catalog.pg_database d WHERE d.datname = 'transaction_db';

-- Grant all privileges on database to postgres if needed
GRANT ALL PRIVILEGES ON DATABASE transaction_db TO postgres;

-- Revoke and regrant public schema permissions
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO postgres;
GRANT USAGE ON SCHEMA public TO transaction_user;
GRANT CREATE ON SCHEMA public TO transaction_user;

-- Now create the tables as postgres user
DROP TABLE IF EXISTS customer_transaction_stats CASCADE;
DROP TABLE IF EXISTS customer_importance_scores CASCADE;
DROP TABLE IF EXISTS eligible_merchants CASCADE;
DROP TABLE IF EXISTS customer_importance CASCADE;
DROP TABLE IF EXISTS processing_log CASCADE;

-- Create customer_transaction_stats table
CREATE TABLE customer_transaction_stats (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    transaction_count BIGINT NOT NULL,
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
    merchant VARCHAR(50) PRIMARY KEY,
    merchant_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    is_eligible BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customer_importance table
CREATE TABLE customer_importance (
    "Source" VARCHAR(255),
    "Target" VARCHAR(255),
    "Weight" DECIMAL(10,6) NOT NULL,
    "typeTrans" VARCHAR(255),
    fraud INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("Source", "typeTrans")
);

-- Create processing_log table
CREATE TABLE processing_log (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    records_processed BIGINT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant all privileges on tables to transaction_user
GRANT ALL PRIVILEGES ON customer_transaction_stats TO transaction_user;
GRANT ALL PRIVILEGES ON customer_importance_scores TO transaction_user;
GRANT ALL PRIVILEGES ON eligible_merchants TO transaction_user;
GRANT ALL PRIVILEGES ON customer_importance TO transaction_user;
GRANT ALL PRIVILEGES ON processing_log TO transaction_user;
GRANT ALL PRIVILEGES ON SEQUENCE processing_log_id_seq TO transaction_user;

-- Grant default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO transaction_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO transaction_user;

-- Insert sample data
INSERT INTO eligible_merchants (merchant, merchant_name, category) VALUES
('M348934600', 'Transportation Service', 'es_transportation'),
('M348934601', 'Food Service', 'es_food'),
('M348934602', 'Health Service', 'es_health');

INSERT INTO customer_importance ("Source", "Target", "Weight", "typeTrans", fraud) VALUES
('C1093826151', 'M348934600', 4.55, 'es_transportation', 0),
('C1093826152', 'M348934601', 3.25, 'es_food', 0),
('C1093826153', 'M348934602', 5.75, 'es_health', 0);

INSERT INTO processing_log (process_name, status, records_processed) VALUES
('initial_setup', 'completed', 0);

-- Verify tables were created
\echo 'Verifying table creation...'
SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('customer_transaction_stats', 'customer_importance_scores', 'eligible_merchants', 'customer_importance', 'processing_log')
ORDER BY table_name;

\echo 'Tables created successfully!'