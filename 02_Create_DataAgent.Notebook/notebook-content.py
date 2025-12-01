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

# # Data Agents Creation and Prep Data for AI


# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install fabric-data-agent-sdk

# CELL ********************

from fabric.dataagent.client import (
    FabricDataAgentManagement,
    create_data_agent,
) 
import sempy_labs as labs
import sempy.fabric as fabric

# CELL ********************

semanticmodel_name = "FCA_Core_SM"
data_agent_name = "FCA_Agent"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use create_data_agent() to create a new agent instance
data_agent = create_data_agent(data_agent_name)

# Use FabricDataAgentManagement() to connect to an existing agent with the same name
#data_agent = FabricDataAgentManagement(data_agent_name)

#delete_data_agent(data_agent_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_agent.add_datasource(semanticmodel_name, type="semanticmodel")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

datasource = data_agent.get_datasources()[0]
datasource.select('calendar')
datasource.select('meters')
datasource.select('fca')
datasource.select('quota_fabric')
datasource.select('reservation_usage')
datasource.select('reservations')
datasource.select('resources')
datasource.select('subscriptions')
datasource.select('capacity_regions')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

user_instructions = "You are a FinOps expert agent that aims to facilitate the understanding and monitoring of the Microsoft Fabric Cost based on the solution Fabric Cost Analysis (FCA). All cost measures, including #PauseCost, are denominated in the currency indicated by the first value present in the BillingCurrency column of the fca table. When answering about a pausing a capacity, make sure to include the #PauseCost. Reservation (RI) are a saving mecanism #Reservation. The number of capacities is available in the #Capacities measure. The RegionName from the table fca contain the region of the different Fabric Capacities deployed. Additional Cost from the mesure #OtherPAYG contain extra cost. Storage cost are avaiblable with the #OneLake measure and also are additional cost. You can save money with Fabric capacity reservation (RI) by committing to a reservation for your Fabric capacity usage for a duration of one year (41% savings). When you purchase a reservation, the Fabric capacity usage that matches the reservation attributes is no longer charged at the pay-as-you-go rates. Meters (Name column from meters table) contain the details of the different activities made on top of the capacities. They are categorized by the column Category from the same table. When answering about witch capacity cost the most and why, you can refer to the #TotalCost measure and showcase the top 3 meters and their related Category and why not suggest if relevant the use of a reservation (RI) to save money. The description of the Meters are available in the column Description from the meters table."
data_agent.update_configuration(
    instructions=user_instructions,
)
data_agent.get_configuration()

# CELL ********************

ds_notes = """ \
All cost measures, including #PauseCost, are denominated in the currency indicated by the first value present in the BillingCurrency column of the fca table.
When answering about a pausing a capacity, make sure to include the #PauseCost.
Reservation (RI) are a saving mecanism #Reservation.
The number of capacities is available in the #Capacities measure.
The RegionName from the table fca contain the region of the different Fabric Capacities deployed.
Additional Cost from the mesure #OtherPAYG contain extra cost.
Storage cost are avaiblable with the #OneLake measure and also are additional cost.
You can save money with Fabric capacity reservation (RI) by committing to a reservation for your Fabric capacity usage for a duration of one year (41% savings).
When you purchase a reservation, the Fabric capacity usage that matches the reservation attributes is no longer charged at the pay-as-you-go rates.
Meters (Name column from meters table) contain the details of the different activities made on top of the capacities. They are categorized by the column Category from the same table.
When answering about witch capacity cost the most and why, you can refer to the #TotalCost measure and showcase the top 3 meters and their related Category and why not suggest if relevant the use of a reservation (RI) to save money.
The description of the Meters are available in the column Description from the meters table.
"""
data_agent.update_configuration(
    instructions=user_instructions,
)
datasource.get_configuration()

# CELL ********************

data_agent.publish()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
