from pyspark.sql.types import *

class TransactionSchemas:
    """Schema definitions for transaction processing"""
    
    TRANSACTION_SCHEMA = StructType([
        StructField("step", IntegerType(), True),
        StructField("customer", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("zipcodeOri", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("zipMerchant", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("fraud", IntegerType(), True)
    ])
    
    CUSTOMER_IMPORTANCE_SCHEMA = StructType([
        StructField("Source", StringType(), True),
        StructField("Target", StringType(), True),
        StructField("Weight", DoubleType(), True),
        StructField("typeTrans", StringType(), True),
        StructField("fraud", IntegerType(), True)
    ])
    
    DETECTION_SCHEMA = StructType([
        StructField("YStartTime", StringType(), True),
        StructField("detectionTime", StringType(), True),
        StructField("patternId", StringType(), True),
        StructField("ActionType", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("merchant", StringType(), True)
    ])
    
    CUSTOMER_STATS_SCHEMA = StructType([
        StructField("customer", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("transaction_count", LongType(), True),
        StructField("merchant_rank", DoubleType(), True)
    ])
    
    IMPORTANCE_SCORES_SCHEMA = StructType([
        StructField("customer", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("avg_weight", DoubleType(), True),
        StructField("weight_rank", DoubleType(), True)
    ])