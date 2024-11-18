"""
Query and Visualization for Drinks Dataset
"""

from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


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


def viz():
    """
    Create visualizations for the drinks dataset.
    """
    query = query_transform()
    count = query.count()
    if count > 0:
        print(f"Data validation passed. {count} rows available.")
    else:
        print("No data available. Please investigate.")
        return

    # Boxplot: Total servings by type
    plt.figure(figsize=(15, 8))
    query.select("beer_servings", 
                 "spirit_servings", 
                 "wine_servings").toPandas().boxplot()
    plt.title("Distribution of Alcohol Servings by Type")
    plt.ylabel("Servings per Capita")
    plt.suptitle("")
    plt.tight_layout()
    plt.show()

    # Bar Chart: Total Litres of Alcohol by Country (Top 10)
    top_countries = query.limit(10).toPandas()
    plt.figure(figsize=(12, 6))
    plt.bar(top_countries["country"], 
            top_countries["total_litres_of_pure_alcohol"], 
            color="orange")
    plt.xlabel("Country")
    plt.ylabel("Total Litres of Pure Alcohol")
    plt.title("Top 10 Countries by Total Alcohol Consumption")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    query_transform()
    viz()
