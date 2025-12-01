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
#from notebookutils import mssparkutils
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
#from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, when, from_json, date_format, lit, row_number,max, lower, regexp_extract
from pyspark.sql.types import StructType, StringType
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

fromMonth = -2 #-1, -2,... from datenow -1 day
toMonth = -1 #-1, -2,... from datenow -1 day

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FUNCTIONS

# CELL ********************

def generateArrayOFPeriod(from_Month: int, to_month: int):
    # Get today's date
    today = datetime.today()

    # Subtract one day
    yesterday = today - timedelta(days=1)
    first_day = yesterday.replace(day=1)

    periodToLoad = []
    if from_Month == to_month:
        periodDate = first_day + relativedelta(months=to_month)
        periodToLoad.append(periodDate.date())
    else:
        for i in range(from_Month, to_month+1):
            periodDate = first_day + relativedelta(months=i)
            periodToLoad.append(periodDate.date())

    return periodToLoad

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## STEP 2 Load Gold:
# 
# - Filter on Fabric data only
# - Remove x_ column
# - Add FabricPause column

# CELL ********************

periodsToLoad = generateArrayOFPeriod(fromMonth, toMonth)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Preparation of predicate for all ingested periods
date_condition = ", ".join([f"'{date.strftime('%Y-%m-%d')}'" for date in periodsToLoad])

print("Start loading Period(s) in " + date_condition)

#clean existing data in silver
if spark.catalog.tableExists("reservation_usage"):
    print("Table exists, snapshot will be clean.")
    Delete_sql_query = f"""DELETE FROM reservation_usage WHERE PeriodLoaded IN ({date_condition})"""
    spark.sql(Delete_sql_query)
    print("Clean performed")

reservation_usage_df = DeltaTable.forPath(spark,"Tables/reservation_usage_silver").toDF()
reservation_usage_df = reservation_usage_df.where(f"""PeriodLoaded IN ({date_condition})""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Filter on Fabric RI usage only
reservation_usage_df=reservation_usage_df.filter(lower(col("InstanceId")).contains("providers/microsoft.fabric/"))\
                                        .withColumn("SubId",regexp_extract(lower(col("InstanceId")), r"subscriptions\/(.*?)\/", 1) )\
                                        .withColumn("ResourceGroupName",regexp_extract(lower(col("InstanceId")), r"resourcegroups\/(.*?)\/", 1) )\
                                        .withColumn("ResourceName",regexp_extract(lower(col("InstanceId")), r"capacities\/(.*?)$", 1) )\
                                        .withColumn("UsageDate_key",date_format("UsageDate", "yyyyMMdd").cast("int"))\
                                        .drop("InstanceId")

reservation_df = DeltaTable.forPath(spark,"Tables/reservations").toDF()
reservation_df = reservation_df.select("ReservationOrderId","ReservationOrderKey")
reservation_usage_df = reservation_usage_df.join(reservation_df,"ReservationOrderId", "leftouter") #retrieve ReservationOrderKey

subscription_df = DeltaTable.forPath(spark,"Tables/subscriptions").toDF()
subscription_df = subscription_df.select("SubId","SubscriptionKey")
reservation_usage_df = reservation_usage_df.join(subscription_df,"SubId", "leftouter") #retrieve SubscriptionKey

resources_df = DeltaTable.forPath(spark,"Tables/resources").toDF()
resources_df = resources_df.select("ResourceName","ResourceKey")
reservation_usage_df = reservation_usage_df.join(resources_df,"ResourceName", "leftouter") #retrieve ResourceKey

reservation_usage_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("reservation_usage")

print("End Loading")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
