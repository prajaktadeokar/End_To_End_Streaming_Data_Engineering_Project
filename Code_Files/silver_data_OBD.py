# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md 
# MAGIC # Schema For staging table
# MAGIC

# COMMAND ----------

df_schema =StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', StringType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC # PArsing event hub data and extracting json to staging table format

# COMMAND ----------

df = spark.read.table('uber_catalog.bronze.rides_raw_data')
df_new = df.withColumn('parsed_rides', from_json(col("rides"), df_schema)).select('parsed_rides.*')

# COMMAND ----------

columns = df_new.columns

# COMMAND ----------


collist= ""
for column in columns:
    collist = collist + "staging_rides." +str(column) + ", "
print(collist)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_catalog.silver_staging.staging_rides

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_catalog.bronze.bulk_rides

# COMMAND ----------

# MAGIC %md 
# MAGIC #Create Jinja Template for Joins

# COMMAND ----------

pip install Jinja2

# COMMAND ----------

jinja_config = [
    {
        "table": "uber_catalog.silver_staging.staging_rides as staging_rides",
        "select": "staging_rides.ride_id, staging_rides.confirmation_number, staging_rides.passenger_id, staging_rides.driver_id, staging_rides.vehicle_id, staging_rides.pickup_location_id, staging_rides.dropoff_location_id, staging_rides.vehicle_type_id, staging_rides.vehicle_make_id, staging_rides.payment_method_id, staging_rides.ride_status_id, staging_rides.pickup_city_id, staging_rides.dropoff_city_id, staging_rides.cancellation_reason_id, staging_rides.passenger_name, staging_rides.passenger_email, staging_rides.passenger_phone, staging_rides.driver_name, staging_rides.driver_rating, staging_rides.driver_phone, staging_rides.driver_license, staging_rides.vehicle_model, staging_rides.vehicle_color, staging_rides.license_plate, staging_rides.pickup_address, staging_rides.pickup_latitude, staging_rides.pickup_longitude, staging_rides.dropoff_address, staging_rides.dropoff_latitude, staging_rides.dropoff_longitude, staging_rides.distance_miles, staging_rides.duration_minutes, staging_rides.booking_timestamp, staging_rides.pickup_timestamp, staging_rides.dropoff_timestamp, staging_rides.base_fare, staging_rides.distance_fare, staging_rides.time_fare, staging_rides.surge_multiplier, staging_rides.subtotal, staging_rides.tip_amount, staging_rides.total_fare, staging_rides.rating",
        "where": ""
        
    },

    {"table": "uber_catalog.bronze.map_vehicle_makes as map_vehicle_makes",
        "select": "map_vehicle_makes.vehicle_make",
        "where": "",
        "on": "staging_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
        },
    {
        "table": "uber_catalog.bronze.map_vehicle_types as map_vehicle_types",
        "select": "map_vehicle_types.vehicle_type",
        "where": "",
        "on": "staging_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    },
    {
        "table": "uber_catalog.bronze.map_cities as map_cities",
        "select": "map_cities.city as pickup_city, map_cities.state, map_cities.region",
        "where": "",
        "on": "staging_rides.pickup_city_id = map_cities.city_id"
    },
    {
        "table": "uber_catalog.bronze.map_cancellation_reasons as map_cancellation_reasons",
        "select": "map_cancellation_reasons.cancellation_reason",
        "where": "",
        "on": "staging_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    },
    {
        "table": "uber_catalog.bronze.map_payment_methods as map_payment_methods",
        "select": "map_payment_methods.payment_method",
        "where": "",
        "on": "staging_rides.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table": "uber_catalog.bronze.map_ride_statuses as map_ride_statuses",
        "select": "map_ride_statuses.ride_status",
        "where": "",
        "on": "staging_rides.ride_status_id = map_ride_statuses.ride_status_id"
    }
    
]

# COMMAND ----------

from jinja2 import Template
jinja_str = '''
SELECT 
  {% for config in jinja_config %}
         {{ config.select }}
           {% if not loop.last %}
              ,
           {% endif %}
        {% endfor %}
FROM 
     {% for config in jinja_config %}
        {% if loop.first %}
           {{ config.table }}
        {% else %}
              LEFT JOIN {{ config.table }} ON {{ config.on }}
        {% endif %} 
      {% endfor %}

    {% for config in jinja_config %}
         {% if loop.first %}
            {% if config.where != "" %}
            WHERE
            {% endif %}
           {% endif %}
           {{ config.where }}
      {% if not loop.last %}
        {% if config.where != "" %}
              AND
           {% endif %}
         {% endif %}   
        {% endfor %}
                   
'''
jinja_template = Template(jinja_str)
rendered_template= jinja_template.render(jinja_config=jinja_config)
print(rendered_template)

# COMMAND ----------


df =spark.sql('select * from uber_catalog.silver_staging.silver_obt')

# COMMAND ----------

df.columns

# COMMAND ----------

display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from uber_catalog.gold.dim_location

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber_catalog.gold.fact

# COMMAND ----------

# MAGIC %md
# MAGIC #Testing Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fact.ride_id, fact.passenger_id, fact.driver_id, fact.vehicle_id, fact.payment_method_id, fact.pickup_city_id,  dim_passenger.passenger_email, dim_passenger.passenger_name, dim_passenger.passenger_phone, fact.distance_miles, fact.duration_minutes, fact.base_fare, fact.distance_fare, fact.time_fare, fact.surge_multiplier, fact.subtotal, fact.tip_amount, fact.rating, fact.total_fare, dim_passenger.passenger_email, dim_passenger.passenger_name, dim_passenger.passenger_phone FROM uber_catalog.gold.fact fact
# MAGIC LEFT JOIN uber_catalog.gold.dim_passenger dim_passenger 
# MAGIC ON fact.passenger_id =dim_passenger.passenger_id

# COMMAND ----------

