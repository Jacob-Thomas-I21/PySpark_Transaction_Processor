-- Drop and recreate all tables with correct schema
-- Run this to fix existing database tables that have wrong column names
-- Run as: psql -h localhost -U postgres -d transaction_db -f setup/drop_and_recreate_tables.sql

-- Drop all existing tables that might have wrong schema
DROP TABLE IF EXISTS customer_transaction_stats CASCADE;
DROP TABLE IF EXISTS customer_importance_scores CASCADE;
DROP TABLE IF EXISTS eligible_merchants CASCADE;
DROP TABLE IF EXISTS customer_importance CASCADE;
DROP TABLE IF EXISTS processing_log CASCADE;

-- Recreate tables with correct CSV-aligned schema
CREATE TABLE customer_transaction_stats (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    transaction_count BIGINT,
    merchant_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

CREATE TABLE customer_importance_scores (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    avg_weight FLOAT,
    weight_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

CREATE TABLE eligible_merchants (
    merchant VARCHAR(255) PRIMARY KEY,
    total_transactions BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

CREATE TABLE processing_log (
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

-- Grant permissions to transaction_user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transaction_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;

-- Insert sample data matching actual CSV structure
INSERT INTO eligible_merchants (merchant, total_transactions) VALUES
('M348934600', 75000),
('M348934601', 60000),
('M348934602', 55000)
ON CONFLICT (merchant) DO NOTHING;

INSERT INTO customer_importance ("Source", "Target", "Weight", "typeTrans", fraud) VALUES
('C1093826151', 'M348934600', 4.55, 'es_transportation', 0),
('C1093826152', 'M348934601', 3.25, 'es_food', 0),
('C1093826153', 'M348934602', 5.75, 'es_health', 0)
ON CONFLICT ("Source", "typeTrans") DO NOTHING;

SELECT 'All tables dropped and recreated with correct CSV-aligned schema!' as status;