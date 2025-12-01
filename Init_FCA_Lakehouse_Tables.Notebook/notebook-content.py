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

# #### FCA Table Initialization
# 
# This notebook initializes all necessary tables for FCA. It should be run after deployment


# CELL ********************

# MAGIC %%configure -f
# MAGIC 
# MAGIC { 
# MAGIC     "defaultLakehouse": { 
# MAGIC         "name":  "FCA"
# MAGIC         }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Definition of static tables

# CELL ********************

# Define the data as a dictionary
capacity_regions = pd.DataFrame(
    [
    ("eastus","United States","US","East US",37.3719,-79.8164,"Virginia"),
    ("westus2","United States","US","West US 2",47.233,-119.852,"Washington"),
    ("australiaeast","Australia","Asia Pacific","Australia East",-33.86,151.2094,"New South Wales"),
    ("southeastasia","Asia Pacific","Asia Pacific","Southeast Asia",1.283,103.833,"Singapore"),
    ("northeurope","Europe","Europe","North Europe",53.3478,-6.2597,"Ireland"),
    ("swedencentral","Sweden","Europe","Sweden Central",60.67488,17.14127,"Gävle"),
    ("uksouth","United Kingdom","Europe","UK South",50.941,-0.799,"London"),
    ("westeurope","Europe","Europe","West Europe",52.3667,4.9,"Netherlands"),
    ("centralus","United States","US","Central US",41.5908,-93.6208,"Iowa"),
    ("southafricanorth","South Africa","Africa","South Africa North",-25.73134,28.21837,"Johannesburg"),
    ("centralindia","India","Asia Pacific","Central India",18.5822,73.9197,"Pune"),
    ("eastasia","Asia Pacific","Asia Pacific","East Asia",22.267,114.188,"Hong Kong"),
    ("indonesiacentral","Indonesia","Asia Pacific","Indonesia Central",-6.2,106.816666,"Jakarta"),
    ("japaneast","Japan","Asia Pacific","Japan East",35.68,139.77,"Tokyo, Saitama"),
    ("japanwest","Japan","Asia Pacific","Japan West",34.6939,135.5022,"Osaka"),
    ("koreacentral","Korea","Asia Pacific","Korea Central",37.5665,126.978,"Seoul"),
    ("malaysiawest","Malaysia","Asia Pacific","Malaysia West",3.140853,101.693207,"Kuala Lumpur"),
    ("newzealandnorth","New Zealand","Asia Pacific","New Zealand North",-36.84853,174.76349,"Auckland"),
    ("canadacentral","Canada","Canada","Canada Central",43.653,-79.383,"Toronto"),
    ("austriaeast","Austria","Europe","Austria East",16.214834,48.2201153,"Vienna"),
    ("francecentral","France","Europe","France Central",46.3772,2.373,"Paris"),
    ("germanywestcentral","Germany","Europe","Germany West Central",50.110924,8.682127,"Frankfurt"),
    ("italynorth","Italy","Europe","Italy North",45.46888,9.18109,"Milan"),
    ("norwayeast","Norway","Europe","Norway East",59.913868,10.752245,"Norway"),
    ("polandcentral","Poland","Europe","Poland Central",52.23334,21.01666,"Warsaw"),
    ("spaincentral","Spain","Europe","Spain Central",40.4259,3.4209,"Madrid"),
    ("switzerlandnorth","Switzerland","Europe","Switzerland North",47.451542,8.564572,"Zurich"),
    ("mexicocentral","Mexico","Mexico","Mexico Central",20.588818,-100.389888,"Querétaro State"),
    ("uaenorth","UAE","Middle East","UAE North",25.266666,55.316666,"Dubai"),
    ("brazilsouth","Brazil","South America","Brazil South",-23.55,-46.633,"Sao Paulo State"),
    ("chilecentral","Chile","South America","Chile Central",-33.447487,-70.673676,"Santiago"),
    ("eastus2euap","Canary (US)","US","East US 2 EUAP",36.6681,-78.3889,""),
    ("israelcentral","Israel","Middle East","Israel Central",31.2655698,33.4506633,"Israel"),
    ("qatarcentral","Qatar","Middle East","Qatar Central",25.551462,51.439327,"Doha"),
    ("brazilus","Brazil","South America","Brazil US",0,0,""),
    ("eastus2","United States","US","East US 2",36.6681,-78.3889,"Virginia"),
    ("eastusstg","Stage (US)","US","East US STG",37.3719,-79.8164,"Virginia"),
    ("southcentralus","United States","US","South Central US",29.4167,-98.5,"Texas"),
    ("westus3","United States","US","West US 3",33.448376,-112.074036,"Phoenix"),
    ("northcentralus","United States","US","North Central US",41.8819,-87.6278,"Illinois"),
    ("westus","United States","US","West US",37.783,-122.417,"California"),
    ("jioindiawest","India","Asia Pacific","Jio India West",22.470701,70.05773,"Jamnagar"),
    ("centraluseuap","Canary (US)","US","Central US EUAP",41.5908,-93.6208,""),
    ("southcentralusstg","Stage (US)","US","South Central US STG",29.4167,-98.5,"Texas"),
    ("westcentralus","United States","US","West Central US",40.89,-110.234,"Wyoming"),
    ("southafricawest","South Africa","Africa","South Africa West",-34.075691,18.843266,"Cape Town"),
    ("australiacentral","Australia","Asia Pacific","Australia Central",-35.3075,149.1244,"Canberra"),
    ("australiacentral2","Australia","Asia Pacific","Australia Central 2",-35.3075,149.1244,"Canberra"),
    ("australiasoutheast","Australia","Asia Pacific","Australia Southeast",-37.8136,144.9631,"Victoria"),
    ("jioindiacentral","India","Asia Pacific","Jio India Central",21.146633,79.08886,"Nagpur"),
    ("koreasouth","Korea","Asia Pacific","Korea South",35.1796,129.0756,"Busan"),
    ("southindia","India","Asia Pacific","South India",12.9822,80.1636,"Chennai"),
    ("westindia","India","Asia Pacific","West India",19.088,72.868,"Mumbai"),
    ("canadaeast","Canada","Canada","Canada East",46.817,-71.217,"Quebec"),
    ("francesouth","France","Europe","France South",43.8345,2.1972,"Marseille"),
    ("germanynorth","Germany","Europe","Germany North",53.073635,8.806422,"Berlin"),
    ("norwaywest","Norway","Europe","Norway West",58.969975,5.733107,"Norway"),
    ("switzerlandwest","Switzerland","Europe","Switzerland West",46.204391,6.143158,"Geneva"),
    ("ukwest","United Kingdom","Europe","UK West",53.427,-3.084,"Cardiff"),
    ("uaecentral","UAE","Middle East","UAE Central",24.466667,54.366669,"Abu Dhabi"),
    ("brazilsoutheast","Brazil","South America","Brazil Southeast",-22.90278,-43.2075,"Rio")
],
    columns=[
        "RegionId",
        "Geography",
        "Continent",
        "FabricRegion",
        "Latitude",
        "Longitude",	
        "Location"
    ]
)

# Create a DataFrame
capacity_regions_df = pd.DataFrame(capacity_regions)

# Write Capacity regions to Lakehouse table
fc_convert_dict = {'RegionId': str,'Geography': str,'Continent': str, 'FabricRegion': str, 'Latitude': float, 'Longitude': float, 'Location': str}
capacity_regions_df = capacity_regions_df.astype(fc_convert_dict)
fc_spark_df = spark.createDataFrame(capacity_regions_df)

fc_spark_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("capacity_regions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Init optional Quota tables
tableName = "quota_fabric"
if spark.catalog.tableExists(tableName):
    print(f"Table {tableName} already exists, nothing to do")
else:
    schema = StructType([
        StructField("RegionId", StringType(), True),
        StructField("Usage", IntegerType(), True),
        StructField("Limit", IntegerType(), True),
        StructField("DateKey", IntegerType(), True),
        StructField("SubscriptionKey", IntegerType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Init optional reservations tables
tableName = "reservations"
if spark.catalog.tableExists(tableName):
    print(f"Table {tableName} already exists, nothing to do")
else:
    schema = StructType([
        StructField("Amount", DoubleType(), True),
        StructField("BillingFrequency", StringType(), True),
        StructField("Currency", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("EventDate", TimestampType(), True),
        StructField("PurchasingSubscriptionGuid", StringType(), True),
        StructField("PurchasingSubscriptionName", StringType(), True),
        StructField("Quantity", DoubleType(), True),
        StructField("Region", StringType(), True),
        StructField("ReservationOrderId", StringType(), True),
        StructField("ReservationOrderName", StringType(), True),
        StructField("ReservationOrderKey", IntegerType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(tableName)

tableName = "reservation_usage"
if spark.catalog.tableExists(tableName):
    print(f"Table {tableName} already exists, nothing to do")
else:
    schema = StructType([
        StructField("ResourceName", StringType(), True),
        StructField("SubAccountId", StringType(), True),
        StructField("ReservationOrderId", StringType(), True),
        StructField("InstanceFlexibilityGroup", StringType(), True),
        StructField("InstanceFlexibilityRatio", DoubleType(), True),
        StructField("Kind", StringType(), True),
        StructField("ReservationId", StringType(), True),
        StructField("ReservedHours", DoubleType(), True),
        StructField("SkuName", StringType(), True),
        StructField("TotalReservedQuantity", DoubleType(), True),
        StructField("UsageDate", TimestampType(), True),
        StructField("UsedHours", DoubleType(), True),
        StructField("PeriodLoaded", DateType(), True),
        StructField("ResourceGroupName", StringType(), True),
        StructField("ReservationOrderKey", IntegerType(), True),
        StructField("SubscriptionKey", IntegerType(), True),
        StructField("ResourceKey", IntegerType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
