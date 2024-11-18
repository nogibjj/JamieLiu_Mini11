"""
Main CLI or app entry point for the Drinks Dataset Pipeline
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query_viz import query_transform, viz
import os

if __name__ == "__main__":
    # Print the current working directory
    current_directory = os.getcwd()
    print(f"Current Directory: {current_directory}")

    # Step 1: Extract dataset and save to DBFS
    print("Starting data extraction...")
    extract()

    # Step 2: Load and transform dataset into a Delta table
    print("Starting data transformation and loading...")
    load()

    # Step 3: Run SQL queries on the Delta table
    print("Running data queries...")
    query_transform()

    # Step 4: Generate visualizations
    print("Generating visualizations...")
    viz()

    print("Pipeline execution completed successfully!")
