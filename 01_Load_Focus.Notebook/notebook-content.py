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
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col, when, from_json, date_format, lit, row_number,max, lower
from pyspark.sql.types import StructType, StringType
from pyspark.sql.window import Window
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

fromMonth = 0 #-1, -2,... from datenow -1 day
toMonth = 0 #-1, -2,... from datenow -1 day
rawSourcePath = "Files/focuscost"#"Files/Costs"#

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FUNCTIONS

# CELL ********************

def find_first_parquet_file(path):
    """
    Recursively search for the first .parquet file in the given directory.
    Args:
        path (str): The root directory to start the search.
    Returns:
        str or None: The full path to the first .parquet file found, or None if not found.
    """
    try:
        for entry in mssparkutils.fs.ls(path):
            if entry.isFile and entry.name.endswith(".parquet"):
                return entry.path
            elif entry.isDir:
                result = find_first_parquet_file(entry.path)
                if result:
                    return result
    except Exception as e:
        print(f"Error accessing {path}: {e}")
    return None

def generate_wildcard_path(full_path: str, raw_source_path: str, current_Date_Folder: str, snapshot_folder: str) -> str:
    # Find the index where the raw source path starts
    idx = full_path.find(raw_source_path)
    if idx == -1:
        raise ValueError("rawSourcePath not found in full path")

    # Extract the base URI before the raw source path
    base_uri = full_path[:idx]
    detailPath = full_path[idx+len(rawSourcePath):]

    # Extract the base URI before the raw source path
    idx = detailPath.find(current_Date_Folder)
    detailPreMonth = detailPath[:idx]  #/fdfd/fdfd
    detailPostMonth = detailPath[idx+len(current_Date_Folder)-1:] # /fdfd/fdfdfd/fdfddf.parquet

    startleveltoAddPre = detailPreMonth.count('/')
    startleveltoAddPost = detailPostMonth.count('/')

    # Construct the wildcard path
    wildcard_path = f"{base_uri}{raw_source_path}{'/*' * startleveltoAddPre}/{snapshot_folder}{'/*' * startleveltoAddPost}.parquet"
    return wildcard_path

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


def AddCapacityPauseColumn(dfsource):
    # Define the schema for the JSON structure. In this version, only for Fabric billingtype
    schema = StructType().add("BillingType", StringType())

    df_parsed = dfsource.withColumn("parsed_json", from_json(col("x_SkuDetails"), schema))

    # Create the new column based on the condition
    df_transformed = df_parsed.withColumn("CapacityPause", when(col("parsed_json.BillingType") == "Capacity Pause/Delete Surcharge", True).otherwise(False))

    # Optionally drop the intermediate parsed column
    df_final = df_transformed.drop("parsed_json")

    return df_final

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

# MARKDOWN ********************

# ## STEP 1 Load Silver:
# Load into bronze table
# Identify context and prepare load in silver
# - Delete previous data
# - Clean date format
# 


# CELL ********************

structurePath = find_first_parquet_file(rawSourcePath)
periodsToLoad = generateArrayOFPeriod(fromMonth, toMonth)

 #identify the date format
current_Date_Folder = ""
print(f"Analyze structurePath to find date pattern: {structurePath}")
match = re.search(r"\/[1-2][0-9][0-9][0-9]\/[0-1][0-9]\/", structurePath)
if match:
    current_Date_Folder = match.group()
    print(f"Find monthly date: {current_Date_Folder}")
    date_pattern = "YYYY/MM"
else:
    match = re.search(r"\/[1-2][0-9][0-9][0-9][0-1][0-9][0-3][0-9]-[1-2][0-9][0-9][0-9][0-1][0-9][0-3][0-9]\/", structurePath)
    if match:
        current_Date_Folder = match.group()
        print(f"Find monthly date: {current_Date_Folder}")
        date_pattern = "YYYYMMDD-YYYYMMDD"

if (current_Date_Folder == ""):
    raise ValueError("No Month Pattern found in structurePath")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for per in periodsToLoad:
    print("Start Period : " + per.strftime("%Y-%m-%d"))

    #drop staging table if exists
    spark.sql("DROP TABLE IF EXISTS focus_staging")


    #generate storage path date part
    if date_pattern == "YYYY/MM":
        snapshot_folder = per.strftime("%Y/%m")
    else:
        fromFormatedDate = per.strftime("%Y%m%d")
        toFormatedDate = (per + relativedelta(months=1) + relativedelta(days=-1)).strftime("%Y%m%d")
        snapshot_folder = fromFormatedDate + "-" + toFormatedDate

    wildcard = generate_wildcard_path(structurePath, rawSourcePath, current_Date_Folder, snapshot_folder)
    print("Used path to load data: " + wildcard)


    try:
        df = spark.read.parquet(wildcard)
        df.write.format('delta').saveAsTable("focus_staging")

        #identify period loaded
        df = spark.sql("SELECT BillingPeriodStart FROM focus_staging LIMIT 1")
        value = df.first()['BillingPeriodStart']

        #clean existing data in silver
        if spark.catalog.tableExists("focus"):
            print("Table exists, snapshot will be clean.")
            spark.sql(f"DELETE FROM focus WHERE BillingPeriodStart = '{value}'")

        #Load data in silver
        focus_staging_df = DeltaTable.forPath(spark,"Tables/focus_staging").toDF()
        focus_staging_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("focus")

    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e):
            print(f"Path not found: {wildcard}")
        else:
            raise # re-raise if it's a different AnalysisException

    print("End Period : " + per.strftime("%Y-%m-%d"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
