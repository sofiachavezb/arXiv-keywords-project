from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import project_config
import os

def process_dataset(category, processed_path):
    spark = SparkSession.builder.getOrCreate()

    # Load the dataset
    category_file = f"{datasets_path}/{category}/dataset_{category}.tsv"
    dataset_df = spark.read.option("header", "true").option("delimiter", "\t").csv(category_file)
    
    # Tokenize abstracts and filter stopwords
    tokenizer = Tokenizer(inputCol="abstract", outputCol="words")
    words_df = tokenizer.transform(dataset_df)
    
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    filtered_df = remover.transform(words_df)
    
    # Count word occurrences
    word_counts = filtered_df.select(explode("filtered_words").alias("word")) \
                             .groupBy("word") \
                             .count() \
                             .orderBy(col("count").desc())
    
    # Save the results
    word_counts_output_path = f"{processed_path}/word_counts_{category}"
    word_counts.write.mode("overwrite").csv(word_counts_output_path)
    
    spark.stop()

# Define paths from project_config.py
datasets_path = project_config.DATASETS_PATH
processed_path = project_config.PROCESSED_DATASET_PATH
categories_file_path = project_config.CATEGORIES_FILES_PATH

# Read categories from the categories file
with open(categories_file_path, 'r') as cf:
    categories = cf.read().splitlines()

# Process each category-specific dataset
for category in categories:
    print(f"Processing dataset for category {category}")
    process_dataset(category, processed_path)
