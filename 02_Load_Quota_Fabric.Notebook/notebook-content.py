# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e4d6e083-58f8-486d-94c0-b826b55c1d5a",
# META       "default_lakehouse_name": "FCA",
# META       "default_lakehouse_workspace_id": "57bceddc-a995-44a7-bfb5-1d5f11ad1e98",
# META       "known_lakehouses": [
# META         {
# META           "id": "e4d6e083-58f8-486d-94c0-b826b55c1d5a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from notebookutils import mssparkutils
from datetime import datetime
from pyspark.sql.functions import explode, lit, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType, NullType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schemaJson = StructType([
    StructField("responses", ArrayType(
        StructType([
            StructField("name", StringType(), True),
            StructField("httpStatusCode", IntegerType(), True),
            StructField("headers", StructType([
                StructField("Pragma", StringType(), True),
                StructField("x-ms-failure-cause", StringType(), True),
                StructField("x-ms-request-id", StringType(), True),
                StructField("x-ms-correlation-request-id", StringType(), True),
                StructField("x-ms-routing-request-id", StringType(), True),
                StructField("Strict-Transport-Security", StringType(), True),
                StructField("X-Content-Type-Options", StringType(), True),
                StructField("X-Cache", StringType(), True),
                StructField("X-MSEdge-Ref", StringType(), True),
                StructField("Cache-Control", StringType(), True),
                StructField("Date", StringType(), True)
            ]), True),
            StructField("content", StructType([
                StructField("error", StructType([
                    StructField("code", StringType(), True),
                    StructField("message", StringType(), True)
                ]), True),
                StructField("value", ArrayType(
                    StructType([
                        StructField("name", StructType([
                            StructField("value", StringType(), True),
                            StructField("localizedValue", StringType(), True)
                        ]), True),
                        StructField("unit", StringType(), True),
                        StructField("currentValue", IntegerType(), True),
                        StructField("limit", IntegerType(), True),
                        StructField("id", StringType(), True)
                    ])
                ), True)
            ]), True),
            StructField("contentLength", IntegerType(), True)
        ])
    ), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def readQuotaJsonFile(filePath, current_date_int):
    fileName = filePath.split('/')[-1]
    SubscriptionKey = int(fileName.split('_')[1])
    df = spark.read.option("multiline", "true").schema(schemaJson).json(filePath)
    df = df.select(explode(df.responses).alias('col'))
    df= df.select("col.name", explode("col.content.value").alias("value"))
    df = df.select(col("name").alias("RegionId"), col("value.currentValue").cast("int").alias("Usage"), col("value.limit").cast("int").alias("Limit"))
    df = df.withColumn("DateKey", lit(current_date_int)).withColumn("SubscriptionKey", lit(SubscriptionKey))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pathQuotas = "Files/Data/Quotas"
quotasListFiles = mssparkutils.fs.ls(pathQuotas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

current_date = int(datetime.now().strftime("%Y%m%d"))

#Clean the date if exists in case of multiple run in the same day. We only keep 1 snapshot per day 
if spark.catalog.tableExists("Quota_fabric"):
     print("Table exists, snapshot will be clean.")
     spark.sql(f"DELETE FROM Quota_fabric WHERE DateKey = {str(current_date)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for Quotafile in quotasListFiles:
    if Quotafile.isFile and Quotafile.name.endswith(".json"):
        print(f"integrate quota data from file {Quotafile.name}")
        df_quota = readQuotaJsonFile(Quotafile.path, current_date)
        df_quota.write.mode("append").saveAsTable("Quota_fabric")

print("Load Done")

#Clean raw files
for Quotafile in quotasListFiles:
    if Quotafile.isFile and Quotafile.name.endswith(".json"):
        mssparkutils.fs.rm(Quotafile.path)

print("Clean Done")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
