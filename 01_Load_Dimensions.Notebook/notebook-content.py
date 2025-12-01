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
from pyspark.sql.functions import row_number,max, lit, lower, regexp_replace
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

source_df = DeltaTable.forPath(spark,"Tables/focus").toDF()
source_df = source_df.where("ServiceName = 'Microsoft.Fabric'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # DIM subcriptions

# CELL ********************

tableName = "subscriptions"
logicalKeyColumn = "SubId"
technicalKeyColumn = "SubscriptionKey"
tableAlreadyExists = spark.catalog.tableExists(tableName)


source_merge_df = source_df.select("SubAccountId","SubAccountName","SubAccountType") \
                            .withColumn("SubId",regexp_replace("SubAccountId","/subscriptions/","")) \
                            .dropDuplicates(["SubId"])

if tableAlreadyExists:
    #Merge to table

    print(f"Merge Data for {tableName} table Started")

    target_table = DeltaTable.forPath(spark, f"Tables/{tableName}")
    target_df = target_table.toDF()
    target_df = target_df.select(logicalKeyColumn,technicalKeyColumn)


    max_key = target_df.agg(max(technicalKeyColumn)).collect()[0][0]

    combined_df = source_merge_df.join(target_df,logicalKeyColumn,"leftouter")
    existingRows_df = combined_df.where(combined_df[technicalKeyColumn].isNotNull())
    newRows_df = combined_df.where(combined_df[technicalKeyColumn].isNull())
    window_spec = Window.orderBy(logicalKeyColumn)
    newRows_df = newRows_df.withColumn(technicalKeyColumn, row_number().over(window_spec) + max_key )

    Src_Merge_df = existingRows_df.union(newRows_df)


    merge = (target_table.alias("target")
        .merge(
            Src_Merge_df.alias("source"),
            f"target.{technicalKeyColumn} = source.{technicalKeyColumn}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        )
    merge.execute()

    print(f"Merge Data for {tableName} Ended")


else:
    print(f"Table {tableName} creation started")
    window_spec = Window.orderBy("SubId")
    source_merge_df = source_merge_df.withColumn(technicalKeyColumn, row_number().over(window_spec))
    source_merge_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(tableName)
    print(f"Table {tableName} creation Ended")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # DIM Resources

# CELL ********************

tableName = "resources"
logicalKeyColumn = "ResourceId"
technicalKeyColumn = "ResourceKey"
tableAlreadyExists = spark.catalog.tableExists(tableName)

source_merge_df = source_df.select("ResourceId","ResourceName","ResourceType","x_ResourceGroupName","ServiceCategory","ServiceName","RegionId") \
                           .withColumnRenamed("x_ResourceGroupName","ResourceGroupName") \
                           .dropDuplicates([logicalKeyColumn])

if tableAlreadyExists:
    #Merge to table

    print(f"Merge Data for {tableName} table Started")

    target_table = DeltaTable.forPath(spark, f"Tables/{tableName}")
    target_df = target_table.toDF()
    target_df = target_df.select(logicalKeyColumn,technicalKeyColumn)


    max_key = target_df.agg(max(technicalKeyColumn)).collect()[0][0]

    combined_df = source_merge_df.join(target_df,logicalKeyColumn,"leftouter")
    existingRows_df = combined_df.where(combined_df[technicalKeyColumn].isNotNull())
    newRows_df = combined_df.where(combined_df[technicalKeyColumn].isNull())
    window_spec = Window.orderBy(logicalKeyColumn)
    newRows_df = newRows_df.withColumn(technicalKeyColumn, row_number().over(window_spec) + max_key )

    Src_Merge_df = existingRows_df.union(newRows_df)


    merge = (target_table.alias("target")
        .merge(
            Src_Merge_df.alias("source"),
            f"target.{technicalKeyColumn} = source.{technicalKeyColumn}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        )
    merge.execute()

    print(f"Merge Data for {tableName} Ended")


else:
    print(f"Table {tableName} creation started")
    window_spec = Window.orderBy(logicalKeyColumn)
    source_merge_df = source_merge_df.withColumn(technicalKeyColumn, row_number().over(window_spec))
    source_merge_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(tableName)
    print(f"Table {tableName} creation Ended")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
