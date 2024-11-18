"""
Query and Visualization for Drinks Dataset in Databricks
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import os


def query_transform():
    """
    Run a SQL query on the drinks dataset to analyze alcohol consumption.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query Drinks Data").getOrCreate()
    query = (
        "SELECT country, beer_servings, spirit_servings, wine_servings, "
        "total_litres_of_pure_alcohol, "
        "(beer_servings + spirit_servings + wine_servings) as total_servings "
        "FROM drinks_delta "
        "ORDER BY total_servings DESC, total_litres_of_pure_alcohol DESC"
    )
    query_result = spark.sql(query)
    return query_result


def viz(output_folder="/dbfs/FileStore/visualizations"):
    """
    Create and save visualizations for the drinks dataset in Databricks.
    """
    # Ensure the output folder exists
    os.makedirs(output_folder, exist_ok=True)

    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")
        return

    # Boxplot: Distribution of Alcohol Servings by Type (Aggregate)
    plt.figure(figsize=(10, 6))
    query.select("beer_servings", "spirit_servings", "wine_servings").toPandas().boxplot()
    plt.title("Distribution of Alcohol Servings by Type")
    plt.ylabel("Servings per Capita")
    plt.tight_layout()
    boxplot_path = os.path.join(output_folder, "boxplot_servings.png")
    plt.savefig(boxplot_path)
    print(f"Boxplot saved to {boxplot_path}")
    plt.close()

    # Bar Chart: Total Litres of Alcohol by Country (Top 10)
    top_countries = query.limit(10).toPandas()
    plt.figure(figsize=(12, 6))
    plt.bar(
        top_countries["country"],
        top_countries["total_litres_of_pure_alcohol"],
        color="orange",
    )
    plt.xlabel("Country")
    plt.ylabel("Total Litres of Pure Alcohol")
    plt.title("Top 10 Countries by Total Alcohol Consumption")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    bar_chart_path = os.path.join(output_folder, "bar_chart_top_countries.png")
    plt.savefig(bar_chart_path)
    print(f"Bar chart saved to {bar_chart_path}")
    plt.close()

    return [boxplot_path, bar_chart_path]


if __name__ == "__main__":
    image_paths = viz()
    for path in image_paths:
        print(f"Visualization available at: {path}")
