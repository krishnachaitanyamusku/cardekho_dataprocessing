# Databricks notebook source
cardekho_raw1_df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("/mnt/cardekho-raw/cardekho_bronze_layer/cars_details_merges.csv")

# COMMAND ----------

cardekho_raw1_df.printSchema()

# COMMAND ----------

display(cardekho_raw1_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType


# Define Schema
cardekho_schema = StructType([
    StructField("position", IntegerType(), True),
    StructField("loc", StringType(), True),
    StructField("manufacturing_year", IntegerType(), True),
    StructField("bt", StringType(), True),
    StructField("tt", StringType(), True),
    StructField("ft", StringType(), True),
    StructField("km", StringType(), True),
    StructField("ip", IntegerType(), True),
    StructField("pi", StringType(), True),
    StructField("images", StringType(), True),
    StructField("imgCount", IntegerType(), True),
    StructField("threesixty", BooleanType(), True),
    StructField("dvn", StringType(), True),
    StructField("oem", StringType(), True),
    StructField("model", StringType(), True),
    StructField("modelId", IntegerType(), True),
    StructField("vid", StringType(), True),
    StructField("centralVariantId", IntegerType(), True),
    StructField("variantName", StringType(), True),
    StructField("city_x", StringType(), True),
    StructField("vlink", StringType(), True),
    StructField("price", StringType(), True),
    StructField("pu", StringType(), True),
    StructField("discountValue", IntegerType(), True),
    StructField("msp", IntegerType(), True),
    StructField("priceSaving", StringType(), True),
    StructField("pageNo", IntegerType(), True),
    StructField("utype", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("usedCarId", IntegerType(), True),
    StructField("usedCarSkuId", StringType(), True),
    StructField("ucid", IntegerType(), True),
    StructField("sid", StringType(), True),
    StructField("tmGaadiStore", BooleanType(), True),
    StructField("emiwidget", StringType(), True),
    StructField("transmissionType", StringType(), True),
    StructField("dynx_itemid_x", IntegerType(), True),
    StructField("dynx_itemid2_x", IntegerType(), True),
    StructField("dynx_totalvalue_x", IntegerType(), True),
    StructField("leadForm", IntegerType(), True),
    StructField("leadFormCta", StringType(), True),
    StructField("offers", StringType(), True),
    StructField("compare", BooleanType(), True),
    StructField("brandingIcon", StringType(), True),
    StructField("pageType", StringType(), True),
    StructField("carType", StringType(), True),
    StructField("corporateId", IntegerType(), True),
    StructField("top_features", StringType(), True),
    StructField("comfort_features", StringType(), True),
    StructField("interior_features", StringType(), True),
    StructField("exterior_features", StringType(), True),
    StructField("safety_features", StringType(), True),
    StructField("Color", StringType(), True),
    StructField("Engine_Type", StringType(), True),
    StructField("Displacement", DoubleType(), True),
    StructField("Max_Power", StringType(), True),
    StructField("Max_Torque", StringType(), True),
    StructField("No_of_Cylinder", DoubleType(), True),
    StructField("Valves_per_Cylinder", DoubleType(), True),
    StructField("Valve_Configuration", StringType(), True),
    StructField("BoreX_Stroke", StringType(), True),
    StructField("Turbo_Charger", StringType(), True),
    StructField("Super_Charger", StringType(), True),
    StructField("Length", StringType(), True),
    StructField("Width", StringType(), True),
    StructField("Height", StringType(), True),
    StructField("Wheel_Base", StringType(), True),
    StructField("Front_Tread", StringType(), True),
    StructField("Rear_Tread", StringType(), True),
    StructField("Kerb_Weight", StringType(), True),
    StructField("Gross_Weight", StringType(), True),
    StructField("Gear_Box", StringType(), True),
    StructField("Drive_Type", StringType(), True),
    StructField("Seating_Capacity", DoubleType(), True),
    StructField("Steering_Type", StringType(), True),
    StructField("Turning_Radius", StringType(), True),
    StructField("Front_Brake_Type", StringType(), True),
    StructField("Rear_Brake_Type", StringType(), True),
    StructField("Top_Speed", StringType(), True),
    StructField("Acceleration", StringType(), True),
    StructField("Tyre_Type", StringType(), True),
    StructField("No_Door_Numbers", DoubleType(), True),
    StructField("Cargo_Volumn", StringType(), True),
    StructField("model_type_new", StringType(),True),
    StructField("originalLocation", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("compare_car_details", StringType(), True),
    StructField("seller_type_new", StringType(), True),
	StructField("seating_capacity_new", StringType(), True),
    StructField("transmission_type", StringType(), True),
    StructField("model_year_new", IntegerType(), True),
    StructField("car_type_new", StringType(), True),
    StructField("model_name",StringType(), True),
	StructField("model_id_new",IntegerType(), True),
	StructField("oem_name",StringType(), True),
	StructField("price_range_segment",StringType(), True),
	StructField("dealer_id_new",IntegerType(), True),
	StructField("state",StringType(), True),
	StructField("city_id_new",IntegerType(), True),
	StructField("fuel_type",StringType(), True),
	StructField("max_engine_capacity_new",StringType(), True),
    StructField("transmission_type_new",StringType(), True),
	StructField("km_driven",IntegerType(), True),
	StructField("model_new",StringType(), True),
	StructField("vehicle_type_new",StringType(), True),
	StructField("brand_name",StringType(), True),
	StructField("engine_cc",StringType(), True),
	StructField("fuel_type_new",StringType(), True),
	StructField("car_segment",StringType(), True),
	StructField("used_car_id",IntegerType(), True),
	StructField("city_name_new",StringType(), True),
	StructField("page_type",StringType(), True),
	StructField("city_y",StringType(), True),
	StructField("engine_capacity_new",StringType(), True),
	StructField("body_type_new",StringType(), True),
	StructField("owner_type_new",StringType(), True),
	StructField("mileage_new",StringType(), True),
	StructField("dealer_id",IntegerType(), True),
	StructField("model_year",IntegerType(), True),
	StructField("variant_name",StringType(), True),
	StructField("price_segment",StringType(), True),
	StructField("dynx_event",StringType(), True),
	StructField("dynx_pagetype",StringType(), True),
	StructField("dynx_itemid_y",IntegerType(), True),
	StructField("dynx_itemid2_y",IntegerType(), True),
	StructField("dynx_totalvalue_y",IntegerType(), True),
	StructField("brand_new",StringType(), True),
	StructField("variant_new",StringType(), True),
	StructField("exterior_color",StringType(), True),
	StructField("min_engine_capacity_new",StringType(), True),
	StructField("owner_type",StringType(), True),
	StructField("price_segment_new",StringType(), True),
	StructField("template_name_new",StringType(), True),
	StructField("page_template",StringType(), True),
	StructField("template_Type_new",StringType(), True),
	StructField("experiment",StringType(), True),
	StructField("Fuel_Suppy_System",StringType(), True),
	StructField("Compression_Ratio",StringType(), True),
	StructField("Alloy_Wheel_Size",StringType(), True),
	StructField("Ground_Clearance_Unladen", StringType(), True)
]
 )

# Load Data with Schema (Example: from CSV)
cardekho_raw1_df = spark.read.format("csv").schema(cardekho_schema).option("header", "true") \
                                                                           .load("/mnt/cardekho-raw/cardekho_bronze_layer/cars_details_merges.csv")


# COMMAND ----------

display(cardekho_raw1_df)

# COMMAND ----------

from pyspark.sql.functions import col,when, regexp_replace, lower, trim,lit,regexp_extract

datatype_cardekho_df = (
    cardekho_raw1_df.withColumnRenamed("loc", "location")
    .withColumn("location", lower(col("location")))
    .withColumnRenamed("bt", "body_type")
    .withColumnRenamed("km", "total_km_driven")
    .withColumn("total_km_driven", regexp_replace(col("total_km_driven"), ",", "").cast("int"))
    .withColumn(
        "price",
        (
            regexp_replace(trim(col("price")), "[^0-9.]", "").cast("double") * 100000
        ).cast("int"),
    )
    .withColumnRenamed("oem", "manufacturer_of_car")
    .withColumnRenamed("vid", "Unique_id_of_the_car")
    .withColumnRenamed("city_x", "city")
    .withColumnRenamed("utype", "type_of_seller")
    .withColumnRenamed("sid", "seller_unique_id")
    .withColumnRenamed("No_Door_Numbers", "No_of_Doors")
    .withColumnRenamed("Alloy_Wheel_Size", "Wheel_Size")
    .withColumn("seating_capacity", col("seating_capacity").cast("int"))
    .withColumn("Turbo_Charger", col("Turbo_Charger").cast("boolean"))
    .withColumn("Super_Charger", col("Super_Charger").cast("boolean"))
    .withColumn("Length", col("Length").cast("double"))
    .withColumn("Width", col("Width").cast("double"))
    .withColumn("Height", col("Height").cast("double"))
    .withColumn("Wheel_Base", col("Wheel_Base").cast("double"))
    .withColumn("Front_Tread", col("Front_Tread").cast("double"))
    .withColumn("Rear_Tread", col("Rear_Tread").cast("double"))
    .withColumn("Kerb_Weight", col("Kerb_Weight").cast("double"))
    .withColumn("Gross_Weight", col("Gross_Weight").cast("double"))
    .withColumn("Turning_Radius", col("Turning_Radius").cast("double"))
    .withColumn("Top_Speed", col("Top_Speed").cast("double"))
    .withColumnRenamed("Cargo_Volumn", "Cargo_Volume")
    .withColumn("Acceleration", col("Acceleration").cast("double"))
    .withColumn("cargo_volume", regexp_replace(col("cargo_volume"), "[^0-9]", ""))
    .withColumn(
        "Ground_Clearance_Unladen",
        regexp_replace(
            trim(col("Ground_Clearance_Unladen").cast("string")), "[^0-9]", ""
        ).cast("double"),
    )
    .withColumn(
        "gear_box",
        regexp_replace(trim(col("gear_box")).cast("string"), "[^0-9]", "").cast("int"),
    )
# Mapping variations to standard categories
    .withColumn(
    "Drive_Type",
    when(trim(lower(col("Drive_Type"))).isin("fwd", "front wheel drive"), "FWD")
    .when(trim(lower(col("Drive_Type"))).isin("rwd", "rear wheel drive", "rear-wheel drive", "rwd(with mtt)","Rear Wheel Drive with ESP","Rear-wheel drive with ESP"), "RWD")
    .when(trim(lower(col("Drive_Type"))).isin("awd", "all wheel drive", "all-wheel drive", "permanent all-wheel drive"), "AWD")
    .when(trim(lower(col("Drive_Type"))).isin("4wd", "4 wd", "4x4","Four Whell Drive","All-wheel drive with Electronic Traction","Permanent all-wheel drive quattro"), "4WD")  # Removed duplicate "4x4"
    .when(trim(lower(col("Drive_Type"))).isin("2wd", "2 wd", "4x2", "two wheel drive","Two Whhel Drive"), "2WD")  # Removed duplicate "4x2"
    .otherwise(lit(None))  # Handle unexpected values
)   .withColumn(
    "Drive_Type",
    when(col("Drive_Type").isNull() & col("Body_Type").isin("Sedan", "Hatchback"), "FWD")
    .when(col("Drive_Type").isNull(), "RWD")  # Default to RWD for other cases
    .otherwise(col("Drive_Type")))
    .withColumn("Max_Power", regexp_extract(col("Max_Power"), r"(\d+\.?\d*)", 1).cast("int")) \
    .withColumn("Max_Torque", regexp_extract(col("Max_Torque"), r"(\d+\.?\d*)", 1).cast("int"))

)




display(datatype_cardekho_df)

# COMMAND ----------

datatype_cardekho_df.select("used_car_id").distinct().count()

# COMMAND ----------

datatype_cardekho_df.count()

# COMMAND ----------

datatype_cardekho_df.filter("location is null")

# COMMAND ----------

datatype_cardekho_df.select("location").distinct().show(70)

# COMMAND ----------

from pyspark.sql.functions import *
datatype_cardekho_df.select("seller_unique_id").groupBy("seller_unique_id").count().orderBy(desc("count")).show()

# COMMAND ----------

datatype_cardekho_df.createOrReplaceTempView("datatype_cardekho")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select manufacturer_of_car,count(*) as total from datatype_cardekho group by manufacturer_of_car order by total desc;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select displacement,count(*) as total from datatype_cardekho where displacement < 800 group by displacement order by total desc;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select body_type,count(*) as total from datatype_cardekho group by body_type order by total desc;

# COMMAND ----------

datatype_cardekho_df.printSchema()

# COMMAND ----------

selected_df = datatype_cardekho_df.select(
    "usedCarSkuId",
    "location", 
    "manufacturing_year",
    "body_type", 
    "transmission_type",
    "fuel_type", 
    "total_km_driven",
    "images",
    "imgCount", 
    "threesixty", 
    "dvn", 
    "manufacturer_of_car", 
    "model",
    "variantName",
    "City", 
    "price",  # Corrected column name 
    "type_of_seller", 
    "carType",
    "top_features", 
    "comfort_features", 
    "interior_features", 
    "exterior_features",
    "safety_features", 
    "Color", 
    "Engine_Type",
    "Displacement",
    "No_of_Cylinder",
    "Valves_per_Cylinder", 
    "Valve_Configuration", 
    "Turbo_Charger",
    "Super_Charger", 
    "Length", 
    "Width", 
    "Height", 
    "Wheel_Base", 
    "Kerb_Weight", 
    "Gross_Weight",
    "Gear_Box", 
    "Drive_Type", 
    "Seating_Capacity", 
    "Steering_Type", 
    "Front_Brake_Type", 
    "Rear_Brake_Type", 
    "Tyre_Type", 
    "No_of_Doors",
    "Cargo_Volume", 
    "state", 
    "exterior_color",
    "owner_type", 
    "Fuel_Suppy_System", 
    "Max_Power",
    "Max_Torque"
)
display(selected_df)

# COMMAND ----------

location_null_df = selected_df.filter("location is null")
display(location_null_df)

# COMMAND ----------

location_filtered_df = selected_df.filter("location is not null")
display(location_filtered_df)

# COMMAND ----------

location_filtered_df.count()

# COMMAND ----------

location_null_df.repartition(1).write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/bad-data/location_null_parquet/")

# COMMAND ----------

location_null_df.repartition(1).write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/bad-data/location_null_csv/")

# COMMAND ----------

wrong_price_df = location_filtered_df.select("*").filter(col("price")  >= 20000000)
display(wrong_price_df)

# COMMAND ----------

wrong_price_filtered_df = location_filtered_df.select("*").filter(col("price")  < 20000000)
display(wrong_price_filtered_df)

# COMMAND ----------

wrong_price_filtered_df.createOrReplaceTempView("wrong_price_filtered")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select manufacturer_of_car,count(*) as no_of_cars,floor(avg(price)) as avg_price from wrong_price_filtered group by manufacturer_of_car order by no_of_cars desc;

# COMMAND ----------

wrong_price_df.repartition(1).write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/bad-data/error_prices_of_car_parquet/")

# COMMAND ----------

wrong_price_df.repartition(1).write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/bad-data/error_prices_of_car_csv/")

# COMMAND ----------

location_filtered_df.select("seating_capacity").groupBy("seating_capacity").count().orderBy("count", ascending=False).show(40)

# COMMAND ----------

seating_missign_df = spark.sql("select * from wrong_price_filtered where seating_capacity is null or seating_capacity = 0")
display(seating_missign_df)

# COMMAND ----------

seating_missign_df.repartition(1).write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/bad-data/seating_capacity_zero_parquet/")

# COMMAND ----------

seating_missign_df.repartition(1).write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/bad-data/seating_capacity_zero_csv/")

# COMMAND ----------

seating_cleaned_df = wrong_price_filtered_df.select("*").filter("seating_capacity is not null and seating_capacity != '0'")
display(seating_cleaned_df)

# COMMAND ----------

seating_cleaned_df.count()

# COMMAND ----------

# i am taking seating cleaned as standard DF for further processing 

# COMMAND ----------

seating_cleaned_df.createOrReplaceTempView("seating_cleaned")

# COMMAND ----------

check_column_list = ["length", "width", "height", "wheel_base","engine_type","top_speed","Alloy_Wheel_Size", "max_torque", "max_power"]

# COMMAND ----------

from pyspark.sql.functions import when, lit, col
from functools import reduce

missed_values_df = seating_cleaned_df.filter(
    reduce(
        lambda x, y: x & y, 
        [col(c).isNull() for c in check_column_list]
    )
)

# COMMAND ----------

display(missed_values_df)

# COMMAND ----------

missed_values_df.repartition(1).write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/bad-data/missed_values_parquet/")

# COMMAND ----------

missed_values_df.repartition(1).write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/bad-data/missed_values_csv/")

# COMMAND ----------

missed_values_df.createOrReplaceTempView("missed_values")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from missed_values;

# COMMAND ----------

missed_values_cleaned_df = spark.sql("select * from seating_cleaned where usedcarskuid not in (select usedcarskuid from missed_values)")
missed_values_cleaned_df.count()

# COMMAND ----------

missed_values_cleaned_df.createOrReplaceTempView("missed_values_cleaned")

# COMMAND ----------

imgcount_cleaned_df = spark.sql("select * from missed_values_cleaned where imgCount != '0'")
display(imgcount_cleaned_df)

# COMMAND ----------

imgcount_cleaned_df.createOrReplaceTempView("imgcount_cleaned")

# COMMAND ----------

imgcount_zero_df = spark.sql("select * from missed_values_cleaned where imgCount=0")
display(imgcount_zero_df)

# COMMAND ----------

imgcount_zero_df.repartition(1).write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/bad-data/imgcount_zero_csv/")

# COMMAND ----------

imgcount_zero_df.repartition(1).write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/bad-data/imgcount_zero_parquet/")

# COMMAND ----------

color_null_df = spark.sql("select * from imgcount_cleaned where color is null")
display(color_null_df)

# COMMAND ----------

color_cleaned_df = spark.sql("select * from imgcount_cleaned where color is not null")
display(color_cleaned_df)
color_cleaned_df.count()

# COMMAND ----------

color_cleaned_df.createOrReplaceTempView("color_cleaned")

# COMMAND ----------

color_null_df.repartition(1).write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/bad-data/color_null_parquet/")

# COMMAND ----------

color_null_df.repartition(1).write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/bad-data/color_null_csv/")

# COMMAND ----------

seating_cleaned_df.count()

# COMMAND ----------

color_cleaned_df.count()

# COMMAND ----------

# this for to check null and odd values in the dataframe
color_cleaned_df.select("gear_box").groupBy("gear_box").count().orderBy("count", ascending=False).show(40)


# COMMAND ----------

spark.sql("select manufacturer_of_car,count(*) as total from color_cleaned group by manufacturer_of_car order by total  desc").show(100)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     state, 
# MAGIC     manufacturer_of_car, 
# MAGIC     COUNT(*) AS cars_sold, 
# MAGIC     dense_rank() OVER (PARTITION BY state ORDER BY COUNT(*) DESC) AS rank
# MAGIC FROM color_cleaned
# MAGIC GROUP BY state, manufacturer_of_car
# MAGIC ORDER BY state, cars_sold DESC;

# COMMAND ----------

color_cleaned_df.select("gear_box").groupBy("gear_box").count().orderBy("count", ascending=False).show(40)

# COMMAND ----------

color_cleaned_df.filter("location is null").count()

# COMMAND ----------

wrong_gearbox_df = color_cleaned_df.filter((color_cleaned_df["gear_box"] > 9) | (color_cleaned_df["gear_box"].isNull()))
display(wrong_gearbox_df)

# COMMAND ----------

wrong_gearbox_df.repartition(1).write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/bad-data/gearbox_null_parquet/")

# COMMAND ----------

wrong_gearbox_df.repartition(1).write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/bad-data/gearbox_null_csv/")

# COMMAND ----------

gear_box_filtered_df = color_cleaned_df.filter((color_cleaned_df["gear_box"] <= 9) & (color_cleaned_df["gear_box"].isNotNull()))

# COMMAND ----------

gear_box_filtered_df.count()

# COMMAND ----------

display(gear_box_filtered_df)

# COMMAND ----------

gear_box_filtered_df.select("Drive_Type").groupBy("Drive_Type").count().orderBy("count", ascending=False).show(40)

# COMMAND ----------

gear_box_filtered_df.createOrReplaceTempView("gear_box_filtered")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select displacement,count(*) as total from gear_box_filtered where displacement < 800 or displacement is null group by displacement order by total desc;

# COMMAND ----------

gear_box_filtered_df.select("*").filter("body_type is null").display()

# COMMAND ----------

body_type_null_df = gear_box_filtered_df.select("*").filter("body_type is null")
display(body_type_null_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gear_box_filtered where dvn like "Maruti Eeco%"

# COMMAND ----------

body_type_cleaned_df = gear_box_filtered_df.withColumn(
    "Body_Type",
    when(col("Body_Type").isNull(), "Minivans").otherwise(col("Body_Type"))
)
display(body_type_cleaned_df)

# COMMAND ----------

from pyspark.sql.functions import year, current_date
car_age_added_df = body_type_cleaned_df.withColumn("car_age", year(current_date()) - body_type_cleaned_df["manufacturing_year"])
display(car_age_added_df)

# COMMAND ----------

datatype_cardekho_df.select("*").groupBy("modelId").count().orderBy("modelId", ascending=False).display()

# COMMAND ----------

car_age_added_df.write.mode("overwrite").format("csv").save("/mnt/cardekho-raw/cardekho_silver_layer/car_age_added_csv/")

# COMMAND ----------

car_age_added_df.write.mode("overwrite").format("parquet").save("/mnt/cardekho-raw/cardekho_silver_layer/car_age_added_parquet/")