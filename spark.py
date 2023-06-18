import requests
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Spark Assignment").getOrCreate()

# Read the data from URL
url = "https://raw.githubusercontent.com/guru99-edu/R-Programming/master/adult_data.csv"
response = requests.get(url)
content = response.text

# Create DataFrame from CSV content
df = spark.read.csv(spark.sparkContext.parallelize(content.splitlines()), header=True, inferSchema=True)

# Register the DataFrame as a temporary table
df.createOrReplaceTempView("adult_data")

# Perform SQL operations

# Query 1: Find the average age of males and females
average_age_query = """
    SELECT gender, AVG(age) AS average_age
    FROM adult_data
    GROUP BY gender
"""
average_age_result = spark.sql(average_age_query)
average_age_result.show()

# Query 2: Find the total number of people where the workclass parameter is 'Private'
private_count_query = """
    SELECT COUNT(*) AS total_count
    FROM adult_data
    WHERE workclass = 'Private'
"""
private_count_result = spark.sql(private_count_query)
private_count_result.show()

# Summarize understanding

# Spark Architecture
spark_architecture_summary = """
Spark follows a distributed computing architecture, consisting of a driver program, a cluster manager,
and multiple executor nodes. The driver program coordinates the execution, the cluster manager allocates
resources, and the executor nodes perform data processing. Spark's data structure, Resilient Distributed
Datasets (RDDs), enables parallel processing and fault tolerance.
"""

# SQL API Benefits
sql_api_benefits_summary = """
The PySpark SQL API provides a familiar SQL-like syntax for interacting with structured data in Spark.
It offers benefits such as:
- Familiar SQL Syntax: Allows leveraging existing SQL skills for data analysis and manipulation.
- Optimizations: Spark's SQL engine optimizes queries for better performance.
- Seamless Integration: The SQL API integrates smoothly with other Spark components and APIs.
- Data Source Flexibility: Supports various data sources and provides a unified interface.
"""

print("Average Age:")
average_age_result.show()

print("Total Count of Private Workclass:")
private_count_result.show()

print("Spark Architecture Summary:")
print(spark_architecture_summary)

print("SQL API Benefits Summary:")
print(sql_api_benefits_summary)
