from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, split
import project_config

def split_dataset(input_path, datasets_path, categories_file_path):
    spark = SparkSession.builder.getOrCreate()

    # Load dataset from JSON
    arxiv_df = spark.read.json(input_path)
    
    # Select relevant columns
    arxiv_df = arxiv_df.select("abstract", "categories")
    
    # Split categories and explode them into individual rows
    arxiv_df = arxiv_df.withColumn("category", explode(split(col("categories"), " ")))
    
    # Get unique categories
    categories = arxiv_df.select("category").distinct().rdd.flatMap(lambda x: x).collect()
    
    # Save the unique categories to a file
    with open(categories_file_path, 'w') as cf:
        for category in categories:
            cf.write(f"{category}\n")
    
    # Split and save dataset by category
    for category in categories:
        category_df = arxiv_df.filter(arxiv_df.category == category)
        category_output_path = f"{datasets_path}/{category}/dataset_{category}.tsv"
        print(f"Saving dataset for category {category} to {category_output_path}")
        category_df.write.option("header", "true").option("delimiter", "\t").mode("overwrite").csv(category_output_path)
    
    spark.stop()
    
    return categories

# Define input and output paths from project_config.py
input_path = project_config.ARXIV_DATASET_PATH
datasets_path = project_config.DATASETS_PATH
categories_file_path = project_config.CATEGORIES_FILES_PATH

# Split the dataset by category
split_dataset(input_path, datasets_path, categories_file_path)
