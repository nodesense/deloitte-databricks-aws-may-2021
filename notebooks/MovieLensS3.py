# Databricks notebook source
access_key = ""
secret_key = ""
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "bucketname"
mount_name = "bucketname" # create a shortcut/mount point /mnt/movielens but this points to S3 storages, DBFS

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))



# COMMAND ----------

display(dbutils.fs.ls("/mnt/bucketname/movielens"))


# COMMAND ----------

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
               .load("/mnt/bucketname/movielens/movies.csv")

movieDf.printSchema()
movieDf.show(1)


ratingDf = spark.read.format("csv")\
               .option("header", True)\
               .schema(ratingSchema)\
               .load("/mnt/bucketname/movielens/ratings.csv")

ratingDf.printSchema()
ratingDf.show(1)

# Fixme: while loading records, if a specific record is corrupted.. handler...

# COMMAND ----------

ratingDf.count() # count of the records

# COMMAND ----------

ratingDf.columns

# COMMAND ----------

# ratingDf.collect() # read from all paritions
ratingDf.take(2) # read data from first partition

# COMMAND ----------

df2 = ratingDf.withColumn("rating_adj", ratingDf.rating + ratingDf["userId"])
df2.show(2)

# COMMAND ----------

ratingDf.select("*").show(1)
ratingDf.select("movieId", "rating" ).show(1)

ratingDf.select(ratingDf.userId, (ratingDf.rating + .02).alias("ratings_adj")).show(1)

# COMMAND ----------

from pyspark.sql.functions import col, min, max, sum # import by names
import pyspark.sql.functions as F # importing all functions as alias F
# col == F.col , F.min, F.max
ratingDf.filter (  (col("rating") >= 3)  & (F.col("rating") <= 4)   ).show(2)

# COMMAND ----------

ratingDf.sort("rating").show(2) # ascending order

# COMMAND ----------

ratingDf.sort(F.desc("rating")).show(2)

# COMMAND ----------

# count the number of ratings by users for given movie
mostPopularDf = ratingDf\
                .groupBy("movieId")\
                .agg(F.count("userId").alias("total_ratings"), F.avg("rating").alias("avg_ratings"))\
                .filter (F.col("total_ratings") >= 100)\
                .sort(F.desc("total_ratings"))

mostPopularDf.show(5)

# COMMAND ----------


