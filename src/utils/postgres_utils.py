import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from config.config import Config
import structlog

logger = structlog.get_logger()

class PostgresUtils:
    def __init__(self):
        self.connection_params = {
            'host': Config.POSTGRES_HOST,
            'port': Config.POSTGRES_PORT,
            'database': Config.POSTGRES_DB,
            'user': Config.POSTGRES_USER,
            'password': Config.POSTGRES_PASSWORD
        }
    
    def get_connection(self):
        """Get PostgreSQL connection"""
        try:
            return psycopg2.connect(**self.connection_params)
        except psycopg2.Error as e:
            logger.error("Failed to connect to PostgreSQL", error=str(e))
            raise
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, params)
                    
                    if cursor.description:  # SELECT query
                        return cursor.fetchall()
                    else:  # INSERT/UPDATE/DELETE
                        conn.commit()
                        return cursor.rowcount
                        
        except psycopg2.Error as e:
            logger.error("Database query failed", query=query, error=str(e))
            raise
    
    def create_tables(self):
        """Create necessary tables for the application"""
        tables = {
            'customer_transaction_stats': '''
                CREATE TABLE IF NOT EXISTS customer_transaction_stats (
                    customer VARCHAR(255),
                    merchant VARCHAR(255),
                    transaction_count BIGINT,
                    merchant_rank FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (customer, merchant)
                )
            ''',
            'customer_importance_scores': '''
                CREATE TABLE IF NOT EXISTS customer_importance_scores (
                    customer VARCHAR(255),
                    merchant VARCHAR(255),
                    avg_weight FLOAT,
                    weight_rank FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (customer, merchant)
                )
            ''',
            'eligible_merchants': '''
                CREATE TABLE IF NOT EXISTS eligible_merchants (
                    merchant VARCHAR(255) PRIMARY KEY,
                    total_transactions BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'customer_importance': '''
                CREATE TABLE IF NOT EXISTS customer_importance (
                    "Source" VARCHAR(255),
                    "Target" VARCHAR(255),
                    "Weight" FLOAT,
                    "typeTrans" VARCHAR(255),
                    fraud INTEGER,
                    PRIMARY KEY ("Source", "typeTrans")
                )
            ''',
            'processing_log': '''
                CREATE TABLE IF NOT EXISTS processing_log (
                    id SERIAL PRIMARY KEY,
                    file_name VARCHAR(500),
                    processing_status VARCHAR(50),
                    detections_found INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
        }
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for table_name, create_sql in tables.items():
                        cursor.execute(create_sql)
                        logger.info("Table created/verified", table=table_name)
                    conn.commit()
                    
        except psycopg2.Error as e:
            logger.error("Failed to create tables", error=str(e))
            raise
    
    def insert_dataframe(self, df, table_name, if_exists='replace'):
        """Insert pandas DataFrame into PostgreSQL table"""
        try:
            # Note: This is a simplified version. In production, use SQLAlchemy
            conn_string = f"postgresql://{Config.POSTGRES_USER}:{Config.POSTGRES_PASSWORD}@{Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}/{Config.POSTGRES_DB}"
            df.to_sql(table_name, conn_string, if_exists=if_exists, index=False, method='multi')
            logger.info("DataFrame inserted into PostgreSQL", table=table_name, rows=len(df))
        except Exception as e:
            logger.error("Failed to insert DataFrame", table=table_name, error=str(e))
            raise