import time
import threading
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.config import Config
from config.spark_config import create_spark_session
from src.mechanism_y.pattern_detector import PatternDetector
from src.mechanism_y.result_writer import ResultWriter
from src.utils.s3_utils import S3Utils
from src.utils.postgres_utils import PostgresUtils
import structlog

logger = structlog.get_logger()

class StreamProcessor:
    def __init__(self):
        try:
            self.spark = create_spark_session("MechanismY_StreamProcessor")
            self.pattern_detector = PatternDetector(self.spark)
            self.result_writer = ResultWriter(self.spark)
            self.s3_utils = S3Utils()
            self.postgres_utils = PostgresUtils()
            
            self.running = False
            self.thread = None
            self.processed_files = set()
            self.customer_importance_df = None
            
            self._load_customer_importance()
            
        except Exception as e:
            logger.error("Error initializing StreamProcessor", error=str(e))
            raise
    
    def _load_customer_importance(self):
        try:
            pass
        except Exception as e:
            logger.warning("Could not load customer importance data", error=str(e))
    
    def start(self):
        if self.running:
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._run_processing, daemon=True)
        self.thread.start()
        logger.info("Stream processor started")
    
    def stop(self):
        self.running = False
        
        if self.thread and self.thread.is_alive():
            try:
                self.thread.join(timeout=8.0)
            except Exception as e:
                logger.warning("Error joining processing thread", error=str(e))
        
        if self.spark:
            try:
                if hasattr(self.spark, '_sc') and self.spark._sc:
                    self.spark._sc.stop()
                self.spark.stop()
                self.spark = None
            except Exception as e:
                logger.warning("Error during Spark cleanup", error=str(e))
                self.spark = None
        
        logger.info("Stream processor stopped")
    
    def _run_processing(self):
        logger.info("Stream processor started - checking for files to process")
        
        while self.running:
            try:
                if not self._is_spark_alive():
                    logger.warning("Spark session not alive, stopping stream processor")
                    break
                
                start_time = time.time()
                new_files = self._get_new_files()
                
                if new_files:
                    logger.info("Found new files to process", count=len(new_files))
                    
                    for file_key in new_files:
                        if not self.running or not self._is_spark_alive():
                            break
                            
                        try:
                            logger.info("Processing file", file=file_key)
                            self._process_file(file_key)
                            self.processed_files.add(file_key)
                            logger.info("Successfully processed file", file=file_key)
                        except Exception as e:
                            logger.error("Error processing file", file=file_key, error=str(e))
                            # Continue processing other files even if one fails
                            continue
                else:
                    logger.debug("No new files found to process",
                               processed_count=len(self.processed_files))
                
                elapsed = time.time() - start_time
                sleep_time = 2.0 - elapsed if elapsed < 2.0 else 1.0  # Increased sleep time
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error("Error in stream processing loop", error=str(e))
                time.sleep(2.0)  # Longer sleep on error
    
    def _get_new_files(self):
        try:
            if not self.running:
                return []
                
            all_files = self.s3_utils.list_files(Config.S3_RAW_DATA_PREFIX)
            new_files = [f for f in all_files if f not in self.processed_files]
            return new_files
        except Exception as e:
            logger.error("Error getting file list from S3", error=str(e))
            return []
    
    def _process_file(self, file_key):
        logger.info("Processing file", file=file_key)
        
        if not self.running or not self._is_spark_alive():
            logger.warning("Skipping file - Spark not available", file=file_key)
            return
        
        try:
            if not self.running or not self.spark:
                logger.warning("Skipping file - not running or no Spark", file=file_key)
                return
            
            logger.info("Loading transaction data from S3", file=file_key)
            transactions_df = self.spark.read.csv(
                f"s3a://{Config.S3_BUCKET_NAME}/{file_key}",
                header=True,
                inferSchema=True
            )
            
            # Check if data was loaded successfully
            try:
                row_count = transactions_df.count()
                logger.info("Loaded transaction data", file=file_key, rows=row_count)
                
                if row_count == 0:
                    logger.warning("No data in file", file=file_key)
                    return
                    
            except Exception as count_error:
                logger.error("Failed to count rows in file", file=file_key, error=str(count_error))
                return
            
            # Load customer importance data
            logger.info("Loading customer importance data")
            self.customer_importance_df = self._load_customer_importance_from_source()
            
            if not self._is_spark_alive():
                logger.warning("Spark died during customer importance loading", file=file_key)
                return
            
            # Run pattern detection
            logger.info("Running pattern detection", file=file_key)
            detections = self.pattern_detector.detect_pattern_id1(
                transactions_df,
                self.customer_importance_df
            )
            
            # Check detection results
            try:
                detection_count = detections.count()
                logger.info("Pattern detection completed", file=file_key, detections=detection_count)
                
                if detection_count > 0:
                    logger.info("Writing detections to S3", file=file_key, count=detection_count)
                    self.result_writer.write_detections(detections)
                    logger.info("Detections written successfully", file=file_key, count=detection_count)
                else:
                    logger.info("No detections found", file=file_key)
                    
            except Exception as detection_error:
                logger.error("Failed to process detections", file=file_key, error=str(detection_error))
                return
                
        except Exception as e:
            error_msg = str(e).lower()
            logger.error("Error processing file", file=file_key, error=str(e))
            
            # Check if it's a critical Spark error
            if any(keyword in error_msg for keyword in ['sparkcontext', 'spark session', 'connection', 'shutdown', 'nullpointerexception']):
                logger.error("Critical Spark error detected, stopping stream processor", error=str(e))
                self.running = False
            else:
                logger.warning("Non-critical error, continuing with other files", error=str(e))
    
    def _load_customer_importance_from_source(self):
        """Load customer importance data from PostgreSQL, create table if it doesn't exist"""
        try:
            # Check if Spark is still alive before attempting database operations
            if not self._is_spark_alive():
                logger.warning("Spark session not available for loading customer importance")
                return self._create_fallback_dataframe()
            
            # First, check if customer_importance table has data
            logger.info("Checking if customer_importance table has data")
            
            # Import the safer JDBC properties
            from config.spark_config import get_postgres_url, get_postgres_properties
            
            # Try to read from PostgreSQL using safer properties
            df = self.spark.read.jdbc(
                url=get_postgres_url(),
                table="customer_importance",
                properties=get_postgres_properties()
            )
            
            logger.info("customer_importance table schema", schema=str(df.schema), columns=df.columns)
            row_count = df.count()
            logger.info("customer_importance table row count", count=row_count)
            
            if row_count == 0:
                logger.warning("customer_importance table is empty, populating it")
                self._create_customer_importance_table()
                
                # Check Spark is still alive before re-reading
                if not self._is_spark_alive():
                    return self._create_fallback_dataframe()
                
                # Re-read after populating
                df = self.spark.read.jdbc(
                    url=get_postgres_url(),
                    table="customer_importance",
                    properties=get_postgres_properties()
                )
                logger.info("Re-read customer_importance after populating", count=df.count())
            
            # Check if it has the expected columns (Source,Target,Weight,typeTrans,fraud)
            expected_cols = ["Source", "Target", "Weight", "typeTrans", "fraud"]
            actual_cols = df.columns
            
            if all(col in actual_cols for col in expected_cols):
                logger.info("Successfully loaded customer_importance table with CSV schema", count=df.count())
                return df
            else:
                logger.warning("customer_importance table has wrong schema",
                             expected=expected_cols,
                             actual=actual_cols)
                raise Exception("Wrong schema in customer_importance table")
                
        except Exception as e:
            logger.error("Error with customer_importance table", error=str(e))
            return self._create_fallback_dataframe()
    
    def _create_fallback_dataframe(self):
        """Create a fallback customer importance DataFrame"""
        try:
            if not self._is_spark_alive():
                logger.warning("Cannot create fallback DataFrame - Spark not available")
                return None
            
            # Create DataFrame with correct schema for immediate use
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
            
            schema = StructType([
                StructField("Source", StringType(), True),
                StructField("Target", StringType(), True),
                StructField("Weight", DoubleType(), True),
                StructField("typeTrans", StringType(), True),
                StructField("fraud", IntegerType(), True)
            ])
            
            # Create sample data matching actual CSV structure
            sample_data = [
                ("C1093826151", "M348934600", 4.55, "es_transportation", 0),
                ("C1093826152", "M348934601", 3.25, "es_food", 0),
                ("C1093826153", "M348934602", 5.75, "es_health", 0),
                ("C1093826154", "M348934603", 2.15, "es_shopping", 0),
                ("C1093826155", "M348934604", 6.85, "es_entertainment", 0)
            ]
            
            df = self.spark.createDataFrame(sample_data, schema)
            logger.info("Created fallback customer importance DataFrame", count=df.count())
            return df
            
        except Exception as e:
            logger.error("Failed to create fallback DataFrame", error=str(e))
            return None
    
    def _create_customer_importance_table(self):
        """Create customer_importance table in PostgreSQL if it doesn't exist"""
        try:
            import psycopg2
            
            # Connect to PostgreSQL
            conn = psycopg2.connect(
                host=Config.POSTGRES_HOST,
                port=Config.POSTGRES_PORT,
                database=Config.POSTGRES_DB,
                user=Config.POSTGRES_USER,
                password=Config.POSTGRES_PASSWORD
            )
            
            cursor = conn.cursor()
            
            # Create table if it doesn't exist - use actual CSV schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS customer_importance (
                    "Source" VARCHAR(255),
                    "Target" VARCHAR(255),
                    "Weight" FLOAT,
                    "typeTrans" VARCHAR(255),
                    fraud INTEGER,
                    PRIMARY KEY ("Source", "typeTrans")
                );
            """)
            
            # Insert sample data matching actual CSV structure
            cursor.execute("""
                INSERT INTO customer_importance ("Source", "Target", "Weight", "typeTrans", fraud) VALUES
                ('C1093826151', 'M348934600', 4.55, 'es_transportation', 0),
                ('C1093826152', 'M348934601', 3.25, 'es_food', 0),
                ('C1093826153', 'M348934602', 5.75, 'es_health', 0),
                ('C1093826154', 'M348934603', 2.15, 'es_shopping', 0),
                ('C1093826155', 'M348934604', 6.85, 'es_entertainment', 0)
                ON CONFLICT ("Source", "typeTrans") DO NOTHING;
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("Successfully created customer_importance table in PostgreSQL")
            
        except Exception as e:
            logger.error("Failed to create customer_importance table", error=str(e))
    
    def _is_spark_alive(self):
        """Check if Spark session is still alive and usable"""
        try:
            # First check if we're even supposed to be running
            if not self.running:
                return False
                
            # Check if spark object exists and is not None
            if not self.spark:
                return False
            
            # Try a simple operation to check if Spark is responsive
            if hasattr(self.spark, '_sc') and self.spark._sc:
                # Check if SparkContext is still active
                if self.spark._sc._jsc is None:
                    return False
                
                # Try a very simple operation
                self.spark._sc.parallelize([1]).count()
                return True
            else:
                return False
                
        except Exception as e:
            # Any exception means Spark is not usable
            logger.debug("Spark health check failed", error=str(e))
            return False
