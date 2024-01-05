from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("My App").getOrCreate()