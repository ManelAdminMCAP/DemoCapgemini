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
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import explode, sequence, to_date, lit
from pyspark.sql.functions import col, year, month, dayofmonth, weekofyear, date_format, to_date, expr, min, max
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Identify perimeter of data
tables_and_dates = [
    ("focus_fabric", "ChargePeriodStart")
]

# List to collect all min dates
min_dates = []
max_dates = []

for table_name, date_col in tables_and_dates:
    if spark.catalog.tableExists(table_name):
        df = spark.table(table_name)
        min_max_dates = df.select(min(date_col).alias("min_date"), max(date_col).alias("max_date")).collect()[0]
        if min_max_dates["min_date"] is not None:
            min_dates.append(min_max_dates["min_date"])
        if min_max_dates["max_date"] is not None:
            max_dates.append(min_max_dates["max_date"])

#add currentDay for quota_fabric Table
max_dates.append(datetime.now())

# Get the overall minimum date
global_min_date = (__builtins__.min(min_dates)) if min_dates else None
global_max_date = (__builtins__.max(max_dates)) if max_dates else None

print(f"Global Min Date across all tables: {global_min_date.strftime('%Y-%m-%d')} - {global_max_date.strftime('%Y-%m-%d')}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameter for date
beginDate = global_min_date.strftime('%Y-%m-%d') #'2023-01-01'
endDate = '2030-12-31'
display_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_df = df.select(
    date_format("date","yyyyMMdd").alias("DateKey").cast("int"),
    date_format("date","yyyy-MM-dd").alias("Date"),
    col("date").alias("Date2Key"),
    year("date").alias("Year"),
    month("date").alias("Month"),
    dayofmonth("date").alias("Day"),
    weekofyear("date").alias("WeekOfYear"),
    date_format("date","yyyy-MM").alias("YearMonth"),
    date_format("date", "E").alias("DayOfWeek")
)

date_df = date_df.createOrReplaceTempView('calendar_temp')

query = """
    SELECT 
        top 1000
        ,DAYOFWEEK(date) AS DayOfWeekNum
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE()) ) THEN 1 ELSE 0 END  AS IsCurrentYear
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE())-1 ) THEN 1 ELSE 0 END  AS IsPreviousYear
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE()) AND QUARTER(date) = QUARTER(CURRENT_DATE()) ) THEN 1 ELSE 0 END  AS IsCurrentQuarter
        ,CASE WHEN ( YEAR(date) = YEAR(CURRENT_DATE()) AND MONTH(date) = MONTH(CURRENT_DATE()) ) THEN 1 ELSE 0 END  AS IsCurrentMonth
        ,CASE WHEN ( DATE_FORMAT(date, 'yyyy-MM') = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -1), 'yyyy-MM') ) THEN 1 ELSE 0 END  AS IsPreviousMonth
        ,CASE WHEN ( date BETWEEN DATE_ADD(CURRENT_DATE(), -14) AND CURRENT_DATE() ) THEN 1 ELSE -1 END  AS IsInLast14Days
        ,CASE WHEN ( date BETWEEN DATE_ADD(CURRENT_DATE(), -30) AND CURRENT_DATE() ) THEN 1 ELSE -1 END  AS IsInLast30Days
    FROM calendar_temp
"""

final_date_df = spark.sql(query)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(final_date_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#add flag with data
df_dataDate = spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{global_max_date.strftime('%Y-%m-%d')}'), interval 1 day)) as date")
df_dataDate = df_dataDate.select(date_format("date","yyyyMMdd").alias("DateKey").cast("int"),lit(1).alias("hasCostData"))
final_date_df = final_date_df.join(df_dataDate,"DateKey", "leftouter").fillna({"hasCostData": 0})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the DataFrame to the lakehouse
#final_date_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the DataFrame to the lakehouse
final_date_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable("calendar")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
