"""
Transform and load function for the drinks dataset
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

def load(dataset="dbfs:/FileStore/mini_proj11/drinks.csv"):
    """
    Loads the drinks dataset, adds transformations, and stores it as a Delta table.
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("Transform and Load Drinks Data").getOrCreate()

    # Load the CSV file
    print(f"Loading dataset from {dataset}...")
    drinks_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # Add a unique ID column
    print("Adding unique ID column...")
    drinks_df = drinks_df.withColumn("id", monotonically_increasing_id())

    # Save the transformed DataFrame as a Delta table
    table_name = "drinks_delta"
    print(f"Writing data to Delta table: {table_name}")
    drinks_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

    # Print row count for verification
    num_rows = drinks_df.count()
    print(f"Number of rows in the dataset: {num_rows}")

    return "Finished transform and load"

if __name__ == "__main__":
    # Path to the drinks dataset in DBFS
    dataset_path = "dbfs:/FileStore/mini_proj11/drinks.csv"
    load(dataset=dataset_path)
