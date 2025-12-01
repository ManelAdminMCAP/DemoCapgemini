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

# MARKDOWN ********************

# ### Check FCA Version
# 
# This notebook checks from Fabric-Toolbox repository the latest version of FCA.
# 
# The FCA_Core_Report will show you, if your current installed FCA version should be updated.

# CELL ********************

# Parameters
display_data = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit
import requests
import json

# Step 1: Fetch the JSON from the public URL
url = "https://raw.githubusercontent.com/Pulsweb/fabric-toolbox/refs/heads/momani/monitoring/fabric-cost-analysis/data/current_fca_version.json"
response = requests.get(url)
data = response.json()

# Step 2: Convert the JSON to a Spark DataFrame
df = spark.createDataFrame([data])

# Optional: Add a timestamp column for tracking
from pyspark.sql.functions import current_timestamp
df = df.withColumn("last_check_timestamp", current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if display_data:
    display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Write the DataFrame to a Delta table in the Lakehouse
df.write.format("delta").mode("overwrite").option("overwriteschema", "true").saveAsTable("audit_latest_available_fca_version")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
