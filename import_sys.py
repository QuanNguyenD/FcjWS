import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Đọc từ Glue Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="raw_db",
    table_name="2025",  # tên table raw JSON
    transformation_ctx="datasource"
)

# Chuyển thành Spark DataFrame để xử lý
df = datasource.toDF()

# Chuyển timestamp → date để làm partition
df = df.withColumn("log_date", to_date(col("timestamp")))

# Ghi ra Processed Zone với định dạng Parquet, partition theo ngày
output_path = "s3://datalake-ecommerce-logs/data/processed/user_log/"

df.write.mode("overwrite") \
    .partitionBy("log_date") \
    .parquet(output_path)

job.commit()
