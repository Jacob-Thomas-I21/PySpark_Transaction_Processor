-- Create missing tables for validation
-- Run this in PostgreSQL after connecting to transaction_db

-- Add missing customer_transaction_stats table (if validation expects different structure)
CREATE TABLE IF NOT EXISTS customer_transaction_stats_validation (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    transaction_count BIGINT NOT NULL,
    merchant_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Add missing customer_importance_scores table (if validation expects different structure)
CREATE TABLE IF NOT EXISTS customer_importance_scores_validation (
    customer VARCHAR(255),
    merchant VARCHAR(255),
    avg_weight FLOAT,
    weight_rank FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer, merchant)
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO transaction_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO transaction_user;

SELECT 'Missing tables created successfully!' as status;