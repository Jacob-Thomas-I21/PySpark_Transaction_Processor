from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, desc, percent_rank, coalesce, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from config.config import Config
from config.spark_config import get_postgres_properties, get_postgres_url
import structlog

logger = structlog.get_logger()

class PatternDetector:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.postgres_props = get_postgres_properties()
        self.postgres_url = get_postgres_url()
    
    def detect_pattern_id1(self, transactions_df, customer_importance_df):
        try:
            customer_stats = self._calculate_customer_stats(transactions_df)
            importance_scores = self._calculate_importance_scores(
                transactions_df, customer_importance_df
            )
            eligible_merchants = self._get_eligible_merchants(transactions_df)
            pattern_matches = self._find_pattern_matches(
                customer_stats, importance_scores, eligible_merchants
            )
            detections = self._create_detection_records(pattern_matches)
            
            logger.info("Pattern PatId1 detection completed",
                       detections_found=detections.count())
            
            return detections
            
        except Exception as e:
            logger.error("Error in pattern detection", error=str(e))
            raise
    
    def _calculate_customer_stats(self, transactions_df):
        customer_stats = transactions_df.groupBy("customer", "merchant") \
            .agg(count("*").alias("transaction_count")) \
            .withColumn(
                "merchant_rank",
                percent_rank().over(
                    Window.partitionBy("merchant")
                    .orderBy(desc("transaction_count"))
                )
            )
        
        logger.info("Customer stats calculated", count=customer_stats.count())
        return customer_stats
    
    def _calculate_importance_scores(self, transactions_df, customer_importance_df):
        logger.info("Using customer_importance_df parameter", schema=str(customer_importance_df.schema))
        logger.info("transactions_df schema in _calculate_importance_scores", schema=str(transactions_df.schema), columns=transactions_df.columns)
        
        transactions_mapped = transactions_df.select(
            col("customer"),
            col("merchant"),
            col("category").alias("transaction_type")
        )
        
        logger.info("transactions_mapped schema", schema=str(transactions_mapped.schema))
        
        joined = transactions_mapped.alias("t").join(
            customer_importance_df.alias("ci"),
            col("t.customer") == col("ci.Source"),
            "left"
        ).select(
            col("t.customer"),
            col("t.merchant"),
            col("t.transaction_type"),
            coalesce(col("ci.Weight"), lit(1.0)).alias("weight")
        )
        
        logger.info("joined schema after using customer_importance_df", schema=str(joined.schema))
        
        importance_scores = joined.groupBy("customer", "merchant") \
            .agg(avg("weight").alias("avg_weight")) \
            .withColumn(
                "weight_rank",
                percent_rank().over(
                    Window.partitionBy("merchant")
                    .orderBy("avg_weight")
                )
            )
        
        logger.info("Importance scores calculated", count=importance_scores.count())
        return importance_scores
    
    def _get_eligible_merchants(self, transactions_df):
        logger.info("transactions_df schema in _get_eligible_merchants", schema=str(transactions_df.schema), columns=transactions_df.columns)
        
        # First get all merchant counts
        all_merchant_counts = transactions_df.groupBy("merchant") \
            .agg(count("*").alias("total_transactions")) \
            .orderBy(col("total_transactions").desc())
        
        total_merchants = all_merchant_counts.count()
        logger.info("Total merchants found", count=total_merchants)
        
        if total_merchants > 0:
            # Show top merchants
            logger.info("Top 5 merchants by transaction count:")
            top_merchants = all_merchant_counts.limit(5).collect()
            for i, row in enumerate(top_merchants):
                logger.info(f"  {i+1}. {row['merchant']}: {row['total_transactions']} transactions")
        
        # Filter by threshold
        merchant_counts = all_merchant_counts.filter(col("total_transactions") > Config.MERCHANT_THRESHOLD)
        eligible_count = merchant_counts.count()
        
        logger.info("Eligible merchants calculated",
                   count=eligible_count,
                   threshold=Config.MERCHANT_THRESHOLD)
        
        if eligible_count == 0:
            logger.warning("No merchants meet the threshold - consider lowering MERCHANT_THRESHOLD",
                         current_threshold=Config.MERCHANT_THRESHOLD,
                         max_transactions=top_merchants[0]['total_transactions'] if top_merchants else 0)
        
        return merchant_counts.select("merchant").collect()
    
    def _find_pattern_matches(self, customer_stats, importance_scores, eligible_merchants):
        logger.info("customer_stats schema in _find_pattern_matches", schema=str(customer_stats.schema))
        logger.info("importance_scores schema in _find_pattern_matches", schema=str(importance_scores.schema))
        
        eligible_merchant_ids = [row["merchant"] for row in eligible_merchants]
        
        if not eligible_merchant_ids:
            logger.warning("No eligible merchants found")
            return self.spark.createDataFrame([], self._get_detection_schema())
        
        filtered_stats = customer_stats.filter(
            col("merchant").isin(eligible_merchant_ids)
        )
        
        filtered_importance = importance_scores.filter(
            col("merchant").isin(eligible_merchant_ids)
        )
        
        pattern_matches = filtered_stats.alias("cs") \
            .join(
                filtered_importance.alias("is"),
                (col("cs.customer") == col("is.customer")) &
                (col("cs.merchant") == col("is.merchant"))
            ) \
            .filter(
                (col("cs.merchant_rank") <= 0.1) &
                (col("is.weight_rank") <= 0.1)
            ) \
            .select(
                col("cs.customer"),
                col("cs.merchant"),
                col("cs.transaction_count"),
                col("is.avg_weight")
            )
        
        return pattern_matches
    
    def _create_detection_records(self, pattern_matches):
        current_time_ist = datetime.now(Config.IST).strftime("%Y-%m-%d %H:%M:%S")

        detections = pattern_matches.withColumn("YStartTime", lit(current_time_ist)) \
            .withColumn("detectionTime", lit(current_time_ist)) \
            .withColumn("patternId", lit("PatId1")) \
            .withColumn("ActionType", lit("UPGRADE")) \
            .select(
                col("YStartTime"),
                col("detectionTime"),
                col("patternId"),
                col("ActionType"),
                col("customer"),
                col("merchant")
            )

        return detections
    
    def _get_detection_schema(self):
        return StructType([
            StructField("YStartTime", StringType(), True),
            StructField("detectionTime", StringType(), True),
            StructField("patternId", StringType(), True),
            StructField("ActionType", StringType(), True),
            StructField("customer", StringType(), True),
            StructField("merchant", StringType(), True)
        ])