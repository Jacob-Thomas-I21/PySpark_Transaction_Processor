-- Fix permissions and create missing tables
-- Run this as postgres superuser: psql -h localhost -U postgres -d transaction_db

-- First, grant schema permissions to transaction_user
GRANT CREATE ON SCHEMA public TO transaction_user;
GRANT USAGE ON SCHEMA public TO transaction_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO transaction_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO transaction_user;

-- Create missing tables that validation expects
CREATE TABLE IF NOT EXISTS customer_transaction_stats (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    transaction_count BIGINT NOT NULL,
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
    merchant VARCHAR(50) PRIMARY KEY,
    merchant_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    is_eligible BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_importance (
    "Source" VARCHAR(255),
    "Target" VARCHAR(255),
    "Weight" DECIMAL(10,6) NOT NULL,
    "typeTrans" VARCHAR(255),
    fraud INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("Source", "typeTrans")
);

CREATE TABLE IF NOT EXISTS processing_log (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL,
    records_processed BIGINT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant all permissions on the new tables to transaction_user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transaction_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;

-- Insert some sample data for testing
INSERT INTO eligible_merchants (merchant, merchant_name, category) VALUES
('M348934600', 'Transportation Service', 'es_transportation'),
('M348934601', 'Food Service', 'es_food'),
('M348934602', 'Health Service', 'es_health')
ON CONFLICT (merchant) DO NOTHING;

INSERT INTO customer_importance ("Source", "Target", "Weight", "typeTrans", fraud) VALUES
('C1093826151', 'M348934600', 4.55, 'es_transportation', 0),
('C1093826152', 'M348934601', 3.25, 'es_food', 0),
('C1093826153', 'M348934602', 5.75, 'es_health', 0)
ON CONFLICT ("Source", "typeTrans") DO NOTHING;

INSERT INTO processing_log (process_name, status, records_processed) VALUES
('initial_setup', 'completed', 0)
ON CONFLICT DO NOTHING;

SELECT 'All missing tables created successfully with proper permissions!' as status;