from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
import project_config

def join_datasets(categories_file_path, processed_datasets_path, joint_datasets_path, min_count_threshold):
    spark = SparkSession.builder.getOrCreate()
    
    with open(categories_file_path, 'r') as cf:
        categories = cf.read().splitlines()
    
    # Initialize an empty DataFrame
    joined_df = None
    
    # Read each category dataset and join them
    for category in categories:
        print(f"Joining dataset for category: {category}")

        category_dir = f"{processed_datasets_path}/word_counts_{category}"
        category_df = spark.read.option("header", "false").csv(category_dir)
        
        # Add column names manually
        category_df = category_df.withColumnRenamed("_c0", "word").withColumnRenamed("_c1", "count")
        
        # Replace '.' with '_dot_' in category names
        category_col = category.replace('.', '_dot_')
        
        # Group by word and sum the counts for the category
        category_df = category_df.groupBy("word").agg(_sum("count").alias(category_col))
        
        if joined_df is None:
            joined_df = category_df
            joined_df = joined_df.withColumn("total", col(category_col))
        else:
            joined_df = joined_df.join(category_df, on="word", how="outer")
            joined_df = joined_df.withColumn("total", col("total") + col(category_col))

        # Liberate memory
        category_df.unpersist()
        del category_df

        print('     Done!')
    
    print("Filling NaNs with 0")
    # Fill null values with 0
    for category in categories:
        category_col = category.replace('.', '_dot_')
        joined_df = joined_df.fillna({category_col: 0})
    
    print("Filtering words with total count below threshold")
    # Filter words with total count below threshold
    filtered_df = joined_df.filter(col("total") >= min_count_threshold)

    # Liberate memory
    joined_df.unpersist()
    del joined_df
    
    # Save the joined dataset
    print("Saving joined dataset")
    filtered_df.write.option("header", "true").mode("overwrite").csv(joint_datasets_path)
    spark.stop()

# Define paths from project_config.py
processed_datasets_path = project_config.HDFS_PROCESSED_DATASETS_PATH
joint_datasets_path = project_config.HDFS_JOINT_DATASETS_PATH
categories_file_path = project_config.LFS_CATEGORIES_FILES_PATH
min_count_threshold = project_config.MIN_COUNT_THRESHOLD

# Join the datasets
join_datasets(categories_file_path, processed_datasets_path, joint_datasets_path, min_count_threshold)
