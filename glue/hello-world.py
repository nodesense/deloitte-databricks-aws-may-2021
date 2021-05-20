import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

logger = glueContext.get_logger()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "gk_orderdb", table_name = "orders", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "gk_orderdb", table_name = "orders", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("order_no", "long", "order_no", "long"), ("amount", "long", "amount", "long"), ("cust_id", "long", "cust_id", "long"), ("country", "string", "country", "string"), ("category", "string", "category", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_no", "long", "order_no", "long"), ("amount", "long", "amount", "long"), ("cust_id", "long", "cust_id", "long"), ("country", "string", "country", "string"), ("category", "string", "category", "string")], transformation_ctx = "applymapping1")

## @type: DropFields
## @args: [paths = ["cust_id"], transformation_ctx = "dropFields1"]
## @return: dropFields1
## @inputs: [frame = applymapping1]
dropFields1 = DropFields.apply(frame = applymapping1, paths = ["cust_id"], transformation_ctx = "dropFields1")

 
## @type: Filter
## @args: [f = filter_function, transformation_ctx = "filterAmount"]
## @return: filterAmount
## @inputs: [frame = dropFields1]
def filter_function(dynamicRecord):
	if dynamicRecord["amount"] >= 400:
		return True
	else:
		return False
filterAmount = Filter.apply(frame = dropFields1, f = filter_function, transformation_ctx = "filterAmount")

## @type: Map
## @args: [f = map_function, transformation_ctx = "adjAmount"]
## @return: adjAmount
## @inputs: [frame = filterAmount]
def map_function(dynamicRecord):
    dynamicRecord["amount"] =  dynamicRecord["amount"] * .9
    return dynamicRecord
    
adjAmount = Map.apply(frame = filterAmount, f = map_function, transformation_ctx = "adjAmount")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://gksworkshop/orders-json"}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = adjAmount]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = adjAmount, connection_type = "s3", connection_options = {"path": "s3://gksworkshop/orders-json"}, format = "json", transformation_ctx = "datasink2")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path":"s3://gksworkshop/orders-parquet"}, format = "parquet",   transformation_ctx = "datasink_s3_parquet"]
## @return: datasink_s3_parquet
## @inputs: [frame = adjAmount]
datasink_s3_parquet = glueContext.write_dynamic_frame.from_options(frame = adjAmount, connection_type = "s3", connection_options = {"path":"s3://gksworkshop/orders-parquet"}, format = "parquet",  transformation_ctx = "datasink_s3_parquet")



# to dataFrame
df1 = adjAmount.toDF()
df1.printSchema()
df1.show()

print("part", df1.rdd.getNumPartitions())


# convert Data Frame to Dynamic Frame

dynamicFrame1 = DynamicFrame.fromDF(df1, glueContext, "dynamicFrame1")
datasink3 = glueContext.write_dynamic_frame.from_options(frame = dynamicFrame1, connection_type = "s3", connection_options = {"path": "s3://gksworkshop/from-df-dyf"}, format = "json", transformation_ctx = "datasink3")

 
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), # orphan record, no matching brand
         (4, 'Pixel', 400),
]

productDf = spark.createDataFrame(data=products, 
                                    schema=["product_id", "name", "brand_id"])

productDf.printSchema() 
productDf.show() # top 20 records


dynamicFrame2 = DynamicFrame.fromDF(productDf, glueContext, "dynamicFrame2")
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dynamicFrame2, connection_type = "s3", connection_options = {"path": "s3://gksworkshop/from-dyf-products"}, format = "xml", transformation_ctx = "datasink4")

# write to s3 via DynamicFrame APIS


#print("My Glue Job Done via print")
#logger.debug("My debug messages")
#logger.info("My Job Info via logger")
#logger.warn("my job warning via logger")
#logger.error("my job errors  via logger")



job.commit()
