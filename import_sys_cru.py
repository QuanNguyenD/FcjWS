import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month
from awsglue.dynamicframe import DynamicFrame

# Lấy tham số JOB_NAME từ Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Khởi tạo GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. Đọc từ Glue Data Catalog (Processed Zone) ---
processed_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="processed_db",    # Đổi tên DB cho đúng
    table_name="user_log",    # Đổi tên bảng cho đúng
    transformation_ctx="processed_dyf"
)

# Chuyển sang Spark DataFrame
df = processed_dyf.toDF()

# --- 2. Xử lý dữ liệu ---
df_transformed = (
    df
    # Bóc tách struct 'amount'
    .withColumnRenamed("amount", "amount_value")
    # Bỏ cột không cần
    .drop("partition_0", "amount")
    # Thêm cột year, month từ log_date
    .withColumn("year", year(col("log_date")))
    .withColumn("month", month(col("log_date")))
)

# --- 3. Chuyển ngược lại thành DynamicFrame ---
curated_dyf = DynamicFrame.fromDF(df_transformed, glueContext, "curated_dyf")

# --- 4. Ghi ra Curated Zone ---
glueContext.write_dynamic_frame.from_options(
    frame=curated_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://datalake-ecommerce-logs/data/curated/user_logs/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet",
    transformation_ctx="datasink"
)

job.commit()
