import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType

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
data_file = sys.argv[1]  
hdfs_ingested_data_path = sys.argv[2] 

####################################
# Read CSV Data from HDFS
####################################
print("######################################")
print("READING RAW CSV FILE FROM HDFS")
print("######################################")

try:
    df_data_csv = (
        spark.read
        .format("csv")
        .option("header", True)
        .load(data_file) 
    )
except Exception as e:
    print(f"Error reading CSV file from HDFS: {e}")
    sys.exit(1)

####################################
# Repartition the Data for Parallel Processing
####################################
print("######################################")
print("REPARTITIONING DATA FOR PARALLEL PROCESSING")
print("######################################")

try:
    # Repartition based on the column 'user_id'
    df_data_csv_repartitioned = df_data_csv.repartition(200, col("user_id"))
except Exception as e:
    print(f"Error repartitioning data: {e}")
    sys.exit(1)

#########################################
# Cast columns to the correct data types
#########################################
print("######################################")
print("CASTING COLUMNS TO CORRECT DATA TYPES")
print("######################################")

try:
    df_data_csv_casted = (
        df_data_csv_repartitioned
        .withColumn('event_time', to_timestamp(col("event_time")))
        .withColumn('event_type', col("event_type").cast(StringType()))
        .withColumn('product_id', col("product_id").cast(IntegerType()))
        .withColumn('category_id', col("category_id").cast(LongType()))
        .withColumn('category_code', col("category_code").cast(StringType()))
        .withColumn('brand', col("brand").cast(StringType()))
        .withColumn('price', col("price").cast(DoubleType()))
        .withColumn('user_id', col("user_id").cast(IntegerType()))
        .withColumn('user_session', col("user_session").cast(StringType()))
    )
except Exception as e:
    print(f"Error casting columns to correct data types: {e}")
    sys.exit(1)

####################################
# Write Ingested Data back to HDFS
####################################
print("######################################")
print(f"WRITING INGESTED DATA TO {hdfs_ingested_data_path}")
print("######################################")

try:
    df_data_csv_casted.write \
        .mode("overwrite") \
        .parquet(hdfs_ingested_data_path)  
    print(f"Data ingested and written to {hdfs_ingested_data_path}")
except Exception as e:
    print(f"Error writing ingested data to HDFS: {e}")
    sys.exit(1)
