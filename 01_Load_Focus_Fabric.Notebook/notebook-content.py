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
from pyspark.sql.functions import col, when, from_json, date_format, lit, row_number,max, lower
from pyspark.sql.types import StructType, StringType
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

fromMonth = 0 #-1, -2,... from datenow -1 day
toMonth = 0 #-1, -2,... from datenow -1 day

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

def AddBillingTypeColumn(dfsource):
    # Define the schema for the JSON structure. In this version, only for Fabric billingtype
    schema = StructType().add("BillingType", StringType())

    df_parsed = dfsource.withColumn("parsed_json", from_json(col("x_SkuDetails"), schema))

    # Create the new column based on the condition
    df_transformed = df_parsed.withColumn("BillingType", col("parsed_json.BillingType"))

    # Optionally drop the intermediate parsed column
    df_final = df_transformed.drop("parsed_json")

    return df_final



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Schema Evolution Function
def updateExistingPastData():
    needToOverwrite = False

    focus_fabric_df = DeltaTable.forPath(spark,"Tables/focus_fabric").toDF()
    if not 'SubscriptionKey' in focus_fabric_df.columns:
        print("Missing SubscriptionKey ---> Adjustment needed")
        subscription_df = DeltaTable.forPath(spark,"Tables/subscriptions").toDF()
        subscription_df = subscription_df.select("SubAccountId","SubscriptionKey")
        # Add a new column
        focus_fabric_df = focus_fabric_df.join(subscription_df,"SubAccountId", "leftouter").drop("SubAccountId","SubAccountName","SubAccountType") #retrieve SubscriptionKey
        needToOverwrite = True

    if not 'ResourceKey' in focus_fabric_df.columns:
        print("Missing ResourceKey ---> Adjustment needed")
        resources_df = DeltaTable.forPath(spark,"Tables/resources").toDF()
        resources_df = resources_df.select("ResourceId","ResourceKey")
        # Add a new column
        focus_fabric_df = focus_fabric_df.join(resources_df,"ResourceId", "leftouter").drop('ResourceId','ResourceName','ResourceType','ServiceCategory','ServiceName','RegionId') #retrieve ResourceKey
        needToOverwrite = True

    if 'RegionName' in focus_fabric_df.columns:
        print("RegionName still exists ---> Adjustment needed")
        focus_fabric_df = focus_fabric_df.drop("RegionName")
        needToOverwrite = True

    if 'x_ResourceGroupName' in focus_fabric_df.columns:
        print("x_ResourceGroupName still exists ---> Adjustment needed")
        focus_fabric_df = focus_fabric_df.drop("x_ResourceGroupName")
        needToOverwrite = True

    if needToOverwrite:
        print("Overwrite the focus_fabric table")
        focus_fabric_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("focus_fabric")
        print("Overwrite done for the focus_fabric table")


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
date_condition = ", ".join([f"'{date.strftime('%Y-%m-%d')} 00:00:00'" for date in periodsToLoad])

print("Start loading Period(s) in " + date_condition)

#clean existing data in silver
if spark.catalog.tableExists("focus_fabric"):
    print("Table exists, snapshot will be clean.")
    Delete_sql_query = f"""DELETE FROM focus_fabric WHERE BillingPeriodStart IN ({date_condition})"""
    spark.sql(Delete_sql_query)
    print("Clean performed")
    updateExistingPastData()

x_columns_to_keep = ['x_SkuMeterName','x_SkuMeterSubcategory']
compute_columns_to_keep = ["BillingType","ChargePeriodStart_DateKey","MeterKey","CommitmentSavings","SubscriptionKey","ResourceKey"]
columns_replace_per_dimensions = ['SubAccountId','SubAccountName','SubAccountType','RegionName','ResourceId','ResourceName','ResourceType','ServiceCategory','ServiceName','RegionId']

focus_df = DeltaTable.forPath(spark,"Tables/focus").toDF()
Meters_df = DeltaTable.forPath(spark,"Tables/meters").toDF().select("Name_Lower","MeterKey")
max_key = Meters_df.agg(max("MeterKey")).collect()[0][0]

# Get all column names that do NOT start with 'x_'
columns_to_keep = [col for col in focus_df.columns if not col.startswith("x_") and col not in columns_replace_per_dimensions]
columns_to_keep = columns_to_keep + x_columns_to_keep + compute_columns_to_keep

focus_df = focus_df.where(f"""ServiceName = 'Microsoft.Fabric' and BillingPeriodStart IN ({date_condition})""")
focus_df = AddBillingTypeColumn(focus_df)

#identify missing Meters in the referencial
missing_Meters_df = focus_df.select("x_SkuMeterName","ChargeDescription","BillingType") \
                            .withColumn("Name_Lower",lower("x_SkuMeterName")) \
                            .drop("x_SkuMeterName") \
                            .dropDuplicates(["Name_Lower"]) \
                            .withColumn("Category",when(col("ChargeDescription") == "Fabric Cap","Fabric CU").otherwise(col("ChargeDescription"))) \
                            .drop("ChargeDescription") \
                            .withColumn("State",lit("Unknow")) \
                            .withColumn("Main",lit("FALSE"))  \
                            .withColumn("Included",when(col("BillingType").isNotNull() & (col("BillingType") != "Capacity Pause/Delete Surcharge"),lit("TRUE")).otherwise(lit("FALSE"))) \
                            .drop("BillingType")

missing_Meters_df = missing_Meters_df.join(Meters_df,"Name_Lower","leftouter")
missing_Meters_df = missing_Meters_df.where(missing_Meters_df.MeterKey.isNull())
window_spec = Window.orderBy("Name_Lower")
missing_Meters_df = missing_Meters_df.withColumn("MeterKey", row_number().over(window_spec) + max_key)

missing_Meters_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("Meters")


Meters_df = DeltaTable.forPath(spark,"Tables/meters").toDF()
Meters_df = Meters_df.select("Name_Lower","MeterKey")
focus_df = focus_df.withColumn("Name_Lower", lower("x_SkuMeterName")).join(Meters_df,"Name_Lower", "leftouter").drop("Name_Lower") #retrieve MeterKey
focus_df = focus_df.withColumn("ChargePeriodStart_DateKey", date_format("ChargePeriodStart", "yyyyMMdd").cast("int"))

subscription_df = DeltaTable.forPath(spark,"Tables/subscriptions").toDF()
subscription_df = subscription_df.select("SubAccountId","SubscriptionKey")
focus_df = focus_df.join(subscription_df,"SubAccountId", "leftouter") #retrieve SubscriptionKey

resources_df = DeltaTable.forPath(spark,"Tables/resources").toDF()
resources_df = resources_df.select("ResourceId","ResourceKey")
focus_df = focus_df.join(resources_df,"ResourceId", "leftouter") #retrieve SubscriptionKey

focus_df = focus_df.withColumn("CommitmentSavings", col("ContractedCost") - col("EffectiveCost"))
focus_df = focus_df.select(columns_to_keep)

focus_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("focus_fabric")

print("End Loading")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
