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

from delta.tables import *
from notebookutils import mssparkutils
from pyspark.sql.functions import row_number,max, lit, lower
from pyspark.sql.window import Window
import requests
import os
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true") # needed for automatic schema evolution in merge

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# URL of the CSV file
csv_url = "https://raw.githubusercontent.com/Pulsweb/fabric-toolbox/refs/heads/momani/monitoring/fabric-cost-analysis/data/Meters.csv"

# Define the target path in the Lakehouse
lakehouse_path = "/lakehouse/default/Files/Data/Meters"
file_name = "Meters.csv"
full_path = f"{lakehouse_path}/{file_name}"

# Create the directory if it doesn't exist
os.makedirs(lakehouse_path, exist_ok=True)

# Download and save the file
response = requests.get(csv_url)
if response.status_code == 200:
    with open(full_path, "wb") as f:
        f.write(response.content)
    print(f"File saved to {full_path}")
else:
    print(f"Failed to download file. Status code: {response.status_code}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meterTableAlreadyExists = spark.catalog.tableExists("Meters")

file_path = "Files/Data/Meters/Meters.csv"
if notebookutils.fs.exists(file_path):
    print(f"✅ File exists: {file_path}")
else:
    print(f"❌ File does not exist: {file_path}")
    if not meterTableAlreadyExists:
        raise Exception("No Meters.csv in the lakehouse and No Meters table already exists")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_df = spark.read.format("csv").option("header","true").load("Files/Data/Meters/Meters.csv")
source_df = source_df.withColumn("Name_Lower",lower("Name"))
source_df = source_df.dropDuplicates(["Name_Lower"])


if meterTableAlreadyExists:
    #Merge to table
    # Load the target Delta table
    print("Merge Data Started")
    target_table = DeltaTable.forPath(spark, "Tables/meters")
    target_df = target_table.toDF()
    target_df = target_df.select("Name_Lower","MeterKey")


    max_key = target_df.agg(max("MeterKey")).collect()[0][0]

    combined_df = source_df.join(target_df,"Name_Lower","leftouter")
    existingRows_df = combined_df.where(combined_df.MeterKey.isNotNull())
    newRows_df = combined_df.where(combined_df.MeterKey.isNull())
    window_spec = Window.orderBy("Name_Lower")
    newRows_df = newRows_df.withColumn("MeterKey", row_number().over(window_spec) + max_key )

    Src_Merge_df = existingRows_df.union(newRows_df)


    merge = (target_table.alias("target")
        .merge(
            Src_Merge_df.alias("source"),
            "target.MeterKey = source.MeterKey"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        )
    merge.execute()

    print("Merge Data Ended")
else:
    print("Table creation started")
    window_spec = Window.orderBy("Name_Lower")
    source_df = source_df.withColumn("MeterKey", row_number().over(window_spec))
    source_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("Meters")
    print("Table creation Ended")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
