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
from pyspark.sql.functions import col, when, from_json, date_format, lit, row_number,max, lower, to_date
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
rawSourcePath = "Files/reservation-details"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FUNCTIONS

# CELL ********************

def find_first_csv_file(path):
    """
    Recursively search for the first .csv file in the given directory.
    Args:
        path (str): The root directory to start the search.
    Returns:
        str or None: The full path to the first .parquet file found, or None if not found.
    """
    try:
        for entry in mssparkutils.fs.ls(path):
            if entry.isFile and entry.name.endswith(".csv"):
                return entry.path
            elif entry.isDir:
                result = find_first_csv_file(entry.path)
                if result:
                    return result
    except Exception as e:
        print(f"Error accessing {path}: {e}")
    return None

def generate_wildcard_path(full_path: str, raw_source_path: str, snapshot_folder: str) -> str:
    # Find the index where the raw source path starts
    idx = full_path.find(raw_source_path)
    if idx == -1:
        raise ValueError("rawSourcePath not found in full path")

    # Extract the base URI before the raw source path
    base_uri = full_path[:idx]
    detailPath = structurePath[idx+len(rawSourcePath):]

    startleveltoAdd = detailPath.count('/') - '/date-date/Guid/name.csv'.count('/')

    # Construct the wildcard path
    wildcard_path = f"{base_uri}{raw_source_path}{'/*' * startleveltoAdd}/{snapshot_folder}/*/*.csv"
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

structurePath = find_first_csv_file(rawSourcePath)
periodsToLoad = generateArrayOFPeriod(fromMonth, toMonth)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for per in periodsToLoad:
    print("Start Period : " + per.strftime("%Y-%m-%d"))

    #drop staging table if exists
    spark.sql("DROP TABLE IF EXISTS reservation_usage_staging")

    #generate storage path date part
    fromFormatedDate = per.strftime("%Y%m%d")
    toFormatedDate = (per + relativedelta(months=1) + relativedelta(days=-1)).strftime("%Y%m%d")
    snapshot_folder = fromFormatedDate + "-" + toFormatedDate

    wildcard = generate_wildcard_path(structurePath, rawSourcePath, snapshot_folder)
    print("Used path to load data: " + wildcard)

    try:
        df = spark.read.csv(wildcard, header=True, inferSchema=True)
        df= df.withColumn("PeriodLoaded",to_date(date_format("UsageDate", "yyyy-MM-01")) )
        df.write.format('delta').saveAsTable("reservation_usage_staging")

        #identify period loaded
        df = spark.sql("SELECT PeriodLoaded FROM reservation_usage_staging LIMIT 1")
        value = df.first()['PeriodLoaded']

        #clean existing data in silver
        if spark.catalog.tableExists('reservation_usage_silver'):
            print("Table exists, snapshot will be clean.")
            spark.sql(f"DELETE FROM reservation_usage_silver WHERE PeriodLoaded = '{value}'")

        #Load data in silver
        ReservationUsage_staging_df = DeltaTable.forPath(spark,"Tables/reservation_usage_staging").toDF()
        display(ReservationUsage_staging_df)
        ReservationUsage_staging_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("reservation_usage_silver")
        print("Data loaded in silver.")

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
