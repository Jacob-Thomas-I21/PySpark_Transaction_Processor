-- Create missing tables as postgres superuser
-- Run this as: psql -h localhost -U postgres -d transaction_db

-- First, let's check and fix the public schema ownership
ALTER SCHEMA public OWNER TO postgres;

-- Create missing tables directly as postgres user
DROP TABLE IF EXISTS customer_transaction_stats CASCADE;
CREATE TABLE customer_transaction_stats (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    transaction_count BIGINT NOT NULL,
    merchant_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

DROP TABLE IF EXISTS customer_importance_scores CASCADE;
CREATE TABLE customer_importance_scores (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    avg_weight FLOAT,
    weight_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

DROP TABLE IF EXISTS eligible_merchants CASCADE;
CREATE TABLE eligible_merchants (
    merchant VARCHAR(50) PRIMARY KEY,
    merchant_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    is_eligible BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS customer_importance CASCADE;
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

DROP TABLE IF EXISTS processing_log CASCADE;
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

-- Change ownership of all tables to transaction_user
ALTER TABLE customer_transaction_stats OWNER TO transaction_user;
ALTER TABLE customer_importance_scores OWNER TO transaction_user;
ALTER TABLE eligible_merchants OWNER TO transaction_user;
ALTER TABLE customer_importance OWNER TO transaction_user;
ALTER TABLE processing_log OWNER TO transaction_user;

-- Grant all privileges explicitly
GRANT ALL PRIVILEGES ON customer_transaction_stats TO transaction_user;
GRANT ALL PRIVILEGES ON customer_importance_scores TO transaction_user;
GRANT ALL PRIVILEGES ON eligible_merchants TO transaction_user;
GRANT ALL PRIVILEGES ON customer_importance TO transaction_user;
GRANT ALL PRIVILEGES ON processing_log TO transaction_user;
GRANT ALL PRIVILEGES ON SEQUENCE processing_log_id_seq TO transaction_user;

-- Insert some sample data for testing
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
SELECT 'Tables created successfully!' as status;
SELECT table_name, table_type FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('customer_transaction_stats', 'customer_importance_scores', 'eligible_merchants', 'customer_importance', 'processing_log');