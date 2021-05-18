# Databricks notebook source
# /FileStore/tables/moviedb/movies.csv
# inferSchema should not be used, scan all the rows to check data type

from pyspark.sql.types import StructType, LongType, DoubleType, IntegerType, StringType
# StructType() new object in python
movieSchema = StructType()\
              .add("movieId", IntegerType(), True)\
              .add("title", StringType(), True)\
              .add('genres', StringType(), True)

ratingSchema = StructType()\
              .add("userId", IntegerType(), True)\
              .add("movieId", IntegerType(), True)\
              .add("rating", DoubleType(), True)\
              .add('timestamp', LongType(), True)

movieDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(movieSchema)\
               .load("/FileStore/tables/moviedb/movies.csv")

movieDf.printSchema()
movieDf.show(1)


ratingDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(ratingSchema)\
               .load("/FileStore/tables/moviedb/ratings.csv")

ratingDf.printSchema()
ratingDf.show(1)

# Fixme: while loading records, if a specific record is corrupted.. handler...

# COMMAND ----------


