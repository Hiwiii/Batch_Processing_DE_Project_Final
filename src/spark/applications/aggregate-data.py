import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, when, col

# Create Spark session
spark = (SparkSession
            .builder
            .config("spark.sql.shuffle.partitions", "200") 
            .config("spark.executor.memory", "4g")  
            .config("spark.executor.cores", "2")  
            .config("spark.driver.memory", "4g")  
            .getOrCreate()
        )

####################################
# Parameters
####################################
hdfs_input_path = sys.argv[1]  
postgres_db = sys.argv[2]
postgres_user = sys.argv[3]
postgres_pwd = sys.argv[4]

####################################
# Read Preprocessed Data from HDFS
####################################
print("######################################")
print("READING PREPROCESSED DATA FROM HDFS")
print("######################################")

try:
    df_preprocessed = spark.read.parquet(hdfs_input_path)
except Exception as e:
    print(f"Error reading data from HDFS: {e}")
    sys.exit(1)

####################################
# Repartition Data for Parallel Processing
####################################
print("######################################")
print("REPARTITIONING DATA FOR PARALLEL PROCESSING")
####################################

# Repartition based on 'category_id' for even 
# distribution of the aggregation workload
df_preprocessed = df_preprocessed.repartition(200, col("category_id"))

####################################
# Aggregate Data
####################################

print("######################################")
print("AGGREGATING DATA")
####################################

try:
    df_aggregated = (
        df_preprocessed.groupBy("category_id", "category_code")
        .agg(
            count("*").alias("total_event_count"),
            count(when(col("event_type") == "purchase", True)).alias("total_purchase_event"),
            count(when(col("event_type") == "cart", True)).alias("total_cart_event"),
            count(when(col("event_type") == "view", True)).alias("total_view_event"),
            avg("price").alias("avg_price")
        )
        .orderBy(col("total_purchase_event").desc())  
    )
except Exception as e:
    print(f"Error during aggregation: {e}")
    sys.exit(1)

########################################
# Write Aggregated Data back to Postgres
########################################

print("######################################")
print("WRITING AGGREGATED DATA TO POSTGRES")
print("######################################")

try:
    df_aggregated.write \
        .format("jdbc") \
        .option("url", postgres_db) \
        .option("dbtable", "public.aggregated_ecommerce_data") \
        .option("user", postgres_user) \
        .option("password", postgres_pwd) \
        .option("batchsize", "10000") \
        .mode("overwrite") \
        .save()
except Exception as e:
    print(f"Error writing data to PostgreSQL: {e}")
    sys.exit(1)

print("Data successfully aggregated and written to PostgreSQL")
