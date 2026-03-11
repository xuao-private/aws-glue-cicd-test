import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # 取输入 DynamicFrame（根据你的 Source 节点名称改这里）
    dynamic_frame = dfc["PostgreSQL_node1772154783764"]

    # 转成 Spark DataFrame
    df = dynamic_frame.toDF()

    # 如果有数据才写出1111
    if df.count() >= 1:
        (
            df.coalesce(1)
              .write
              .option("header", "false")   # 不输出列名
              .option("quote", '"')        # 字段用双引号
              .mode("overwrite")
              .csv("s3://xuao-s3-test1/gluetest/")
        )

    # 返回 DynamicFrameCollection，供下游节点使用
    return DynamicFrameCollection({"dynamic_frame_out": DynamicFrame.fromDF(df, glueContext, "dynamic_frame_out")}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node PostgreSQL
PostgreSQL_node1772154783764 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "members",
        "connectionName": "Jdbc connection",
    },
    transformation_ctx = "PostgreSQL_node1772154783764"
)

# Script generated for node Custom Transform
CustomTransform_node1772183129684 = MyTransform(glueContext, DynamicFrameCollection({"PostgreSQL_node1772154783764": PostgreSQL_node1772154783764}, glueContext))

# Script generated for node Amazon S3
if (PostgreSQL_node1772154783764.count() >= 1):
   PostgreSQL_node1772154783764 = PostgreSQL_node1772154783764.coalesce(1)
AmazonS3_node1772159602962 = glueContext.write_dynamic_frame.from_options(frame=PostgreSQL_node1772154783764, connection_type="s3", format="csv", connection_options={"path": "s3://xuao-s3-test1/gluetest/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1772159602962")

job.commit()