import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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
hdfs_output_path = sys.argv[2]  

####################################
# Read Data from HDFS
####################################
print("######################################")
print("READING INGESTED DATA FROM HDFS")
print("######################################")

try:
    df_data = (
        spark.read
        .parquet(hdfs_input_path)  
    )
except Exception as e:
    print(f"Error reading data from HDFS: {e}")
    sys.exit(1)

####################################
# Preprocess Data (Filtering)
####################################
print("######################################")
print("PREPROCESSING DATA")
print("######################################")

try:
    df_preprocessed = df_data.filter(
        (df_data["event_type"] != "[null]") &
        (df_data["product_id"].isNotNull()) &
        (df_data["category_id"].isNotNull()) & 
        (df_data["category_code"] != "[null]") &
        (df_data["brand"] != "[null]") &
        (df_data["price"].isNotNull()) &
        (df_data["user_id"].isNotNull()) &
        (df_data["user_session"] != "[null]")
    )
except Exception as e:
    print(f"Error during data preprocessing: {e}")
    sys.exit(1)

####################################
# Repartition the Data for Better Parallel Processing
####################################
print("######################################")
print("REPARTITIONING DATA FOR PARALLEL PROCESSING")
print("######################################")

try:
    df_preprocessed = df_preprocessed.repartition(200, col("user_id"))
except Exception as e:
    print(f"Error during data repartitioning: {e}")
    sys.exit(1)

####################################
# Write Preprocessed Data to HDFS
####################################
print("######################################")
print("WRITING PREPROCESSED DATA TO HDFS")
print("######################################")

try:
    df_preprocessed.write \
        .mode("overwrite") \
        .parquet(hdfs_output_path)  
    print(f"Data preprocessed and written to {hdfs_output_path}")
except Exception as e:
    print(f"Error writing preprocessed data to HDFS: {e}")
    sys.exit(1)
