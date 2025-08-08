from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from config.config import Config
from config.spark_config import get_postgres_url, get_postgres_properties
from src.utils.s3_utils import S3Utils
import structlog

logger = structlog.get_logger()

class ResultWriter:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.s3_utils = S3Utils()
        self.detection_counter = 0
    
    def write_detections(self, detections_df):
        """Write detections to S3 in batches of 50 and also to PostgreSQL"""
        try:
            # Check if we have any detections
            detection_count = detections_df.count()
            if detection_count == 0:
                logger.info("No detections to write")
                return
            
            logger.info("Writing detections to S3", count=detection_count)
            
            # Convert to list for batching
            detections_list = detections_df.collect()
            
            # Process in batches of 50
            batch_size = Config.DETECTION_BATCH_SIZE
            total_detections = len(detections_list)
            successful_batches = 0
            
            for i in range(0, total_detections, batch_size):
                batch = detections_list[i:i + batch_size]
                try:
                    self._write_batch(batch)
                    successful_batches += 1
                except Exception as e:
                    logger.error("Failed to write batch", batch_index=i//batch_size, error=str(e))
                    # Continue with other batches
                    continue
                
            # Optionally write to PostgreSQL for persistence (only if table exists)
            try:
                self._write_to_postgres(detections_df)
            except Exception as e:
                logger.warning("PostgreSQL write failed, continuing with S3-only storage", error=str(e))
                
            logger.info("Detection writing completed",
                       total_detections=total_detections,
                       successful_batches=successful_batches,
                       total_batches=((total_detections - 1) // batch_size) + 1)
                       
        except Exception as e:
            logger.error("Error writing detections", error=str(e))
            raise
    
    def _write_batch(self, batch):
        """Write a single batch of detections to S3"""
        try:
            # Create DataFrame from batch
            batch_df = self.spark.createDataFrame(batch)
            
            # Generate unique filename
            timestamp = datetime.now(Config.IST).strftime("%Y%m%d_%H%M%S_%f")[:-3]
            self.detection_counter += 1
            filename = f"detections_{timestamp}_{self.detection_counter:04d}.csv"
            
            # Write to S3
            s3_key = f"{Config.S3_DETECTIONS_PREFIX}{filename}"
            
            # Convert to Pandas for easier S3 upload
            batch_pandas = batch_df.toPandas()
            self.s3_utils.upload_dataframe(batch_pandas, s3_key)
            
            logger.info("Detection batch written", 
                       filename=filename, 
                       batch_size=len(batch),
                       s3_key=s3_key)
                       
        except Exception as e:
            logger.error("Error writing detection batch", error=str(e))
            raise
    
    def _write_to_postgres(self, detections_df):
        """Write detections to PostgreSQL using safe append mode - only if table exists"""
        try:
            # First check if the table exists by trying to read from it
            test_df = self.spark.read.jdbc(
                url=get_postgres_url(),
                table="pattern_detections",
                properties=get_postgres_properties()
            )
            # If we get here, table exists, so we can write to it
            
            # Add timestamp for tracking
            detections_with_timestamp = detections_df.withColumn(
                "detection_timestamp",
                current_timestamp()
            )
            
            # Write to PostgreSQL using append mode (no table dropping)
            detections_with_timestamp.write \
                .mode("append") \
                .jdbc(
                    url=get_postgres_url(),
                    table="pattern_detections",
                    properties=get_postgres_properties()
                )
            
            logger.info("Detections written to PostgreSQL", count=detections_df.count())
            
        except Exception as e:
            logger.info("PostgreSQL write skipped - table may not exist yet", error=str(e))
            # Don't raise - S3 writing is the primary method