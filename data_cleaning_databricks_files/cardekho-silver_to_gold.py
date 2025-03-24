# Databricks notebook source
cardekho_cleaned_df = spark.read.parquet("/mnt/cardekho-raw/cardekho_silver_layer/car_age_added_parquet/")

# COMMAND ----------

cardekho_cleaned_df.printSchema()

# COMMAND ----------

display(cardekho_cleaned_df)

# COMMAND ----------

cardekho_cleaned_df.createOrReplaceTempView("cardekho_cleaned")

# COMMAND ----------

high_budget_grouped_df = spark.sql("select manufacturer_of_car,count(*) as total from cardekho_cleaned where price >= 2000000 group by manufacturer_of_car order by total desc")
display(high_budget_grouped_df)

# COMMAND ----------

high_budget_cars_df = spark.sql("select * from cardekho_cleaned where price >= 2000000")
display(high_budget_cars_df)

# COMMAND ----------

high_budget_cars_df.printSchema()

# COMMAND ----------

high_budget_cars_df.repartition(1).write.mode("overwrite").format('parquet').save("/mnt/cardekho-raw/cardekho_gold_layer/high_budget_cars_parquet/")

# COMMAND ----------

high_budget_cars_df.repartition(1).write.mode("overwrite").format('csv').save("/mnt/cardekho-raw/cardekho_gold_layer/high_budget_cars_csv/")

# COMMAND ----------

mid_range_budget_cars_df = spark.sql("select * from cardekho_cleaned where price between 1000000 and 1999999")
display(mid_range_budget_cars_df)

# COMMAND ----------

mid_range_budget_cars_df.repartition(1).write.mode("overwrite").format('parquet').save("/mnt/cardekho-raw/cardekho_gold_layer/mid_range_budget_cars_parquet/")

# COMMAND ----------

mid_range_budget_cars_df.repartition(1).write.mode("overwrite").format('csv').save("/mnt/cardekho-raw/cardekho_gold_layer/mid_range_budget_cars_csv/")

# COMMAND ----------

low_budget_cars_df = spark.sql("select * from cardekho_cleaned where price between 0 and 999999")
display(low_budget_cars_df)

# COMMAND ----------

low_budget_cars_df.repartition(1).write.mode("overwrite").format('parquet').save("/mnt/cardekho-raw/cardekho_gold_layer/low_budget_cars_parquet/")

# COMMAND ----------

low_budget_cars_df.repartition(1).write.mode("overwrite").format('csv').save("/mnt/cardekho-raw/cardekho_gold_layer/low_budget_cars_csv/")

# COMMAND ----------

from pyspark.sql.functions import count

cardekho_cleaned_df.groupBy("car_age") \
    .agg(count("*").alias("count")) \
    .orderBy("count", ascending=False) \
    .select("car_age", "count") \
    .show()

# COMMAND ----------

# These cars city driven These prioritize fuel efficiency and maneuverability
low_power_cars_df = spark.sql("select * from cardekho_cleaned where max_power between 32 and 100")
display(low_power_cars_df)

# COMMAND ----------

low_power_cars_df.repartition(1).write.mode("overwrite").format('parquet').save("/mnt/cardekho-raw/cardekho_gold_layer/low_power_cars_parquet/")

# COMMAND ----------

low_power_cars_df.repartition(1).write.mode("overwrite").format('csv').save("/mnt/cardekho-raw/cardekho_gold_layer/low_power_cars_csv/")

# COMMAND ----------

# These offer a balance between performance and practicality
Mid_performance_cars_df = spark.sql("select * from cardekho_cleaned where max_power between 100 and 200 ")
display(Mid_performance_cars_df)

# COMMAND ----------

Mid_performance_cars_df.repartition(1).write.mode("overwrite").format('parquet').save("/mnt/cardekho-raw/cardekho_gold_layer/Mid_performance_cars_parquet/")

# COMMAND ----------

Mid_performance_cars_df.repartition(1).write.mode("overwrite").format('csv').save("/mnt/cardekho-raw/cardekho_gold_layer/Mid_performance_cars_csv/")

# COMMAND ----------

#These are designed for speed and acceleration, often featuring engines with 200+ horsepower and high torque outputs
high_performance_cars_df = spark.sql("select * from cardekho_cleaned where max_power > 200 ")
display(high_performance_cars_df)

# COMMAND ----------

high_performance_cars_df.repartition(1).write.mode("overwrite").format('parquet').save("/mnt/cardekho-raw/cardekho_gold_layer/high_performance_cars_parquet/")

# COMMAND ----------

high_performance_cars_df.repartition(1).write.mode("overwrite").format('csv').save("/mnt/cardekho-raw/cardekho_gold_layer/high_performance_cars_csv/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cardekho_cleaned where car_age between 1 and 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cardekho_cleaned where car_age between 5 and 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cardekho_cleaned where car_age between 11 and 25;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     state, 
# MAGIC     manufacturer_of_car, 
# MAGIC     COUNT(*) AS cars_for_sale, 
# MAGIC     dense_rank() OVER (PARTITION BY state ORDER BY COUNT(*) DESC) AS rank
# MAGIC FROM cardekho_cleaned
# MAGIC GROUP BY state, manufacturer_of_car
# MAGIC ORDER BY state, cars_for_sale DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC with cardekho_cte as (
# MAGIC SELECT 
# MAGIC     state, 
# MAGIC     body_type, 
# MAGIC     COUNT(*) AS cars_for_sale, 
# MAGIC     dense_rank() OVER (PARTITION BY state ORDER BY COUNT(*) DESC) AS rank
# MAGIC FROM cardekho_cleaned
# MAGIC GROUP BY state, body_type
# MAGIC ORDER BY state, cars_for_sale DESC
# MAGIC )
# MAGIC SELECT * FROM cardekho_cte where rank <= 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cardekho_manufacturer_cte AS (
# MAGIC SELECT 
# MAGIC     state, 
# MAGIC     manufacturer_of_car, 
# MAGIC     COUNT(*) AS cars_for_sale, 
# MAGIC     dense_rank() OVER (PARTITION BY state ORDER BY COUNT(*) DESC) AS rank
# MAGIC FROM cardekho_cleaned
# MAGIC GROUP BY state, manufacturer_of_car
# MAGIC ORDER BY state, cars_for_sale DESC
# MAGIC )
# MAGIC SELECT * FROM cardekho_manufacturer_cte where rank <= 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH avg_km AS (
# MAGIC     SELECT 
# MAGIC         manufacturer_of_car,
# MAGIC         car_age,
# MAGIC         AVG(total_km_driven) AS avg_km_driven
# MAGIC     FROM 
# MAGIC         cardekho_cleaned
# MAGIC     GROUP BY 
# MAGIC         manufacturer_of_car, 
# MAGIC         car_age
# MAGIC )
# MAGIC SELECT 
# MAGIC     manufacturer_of_car,
# MAGIC     car_age,
# MAGIC     avg_km_driven
# MAGIC FROM 
# MAGIC     avg_km
# MAGIC WHERE 
# MAGIC     avg_km_driven >= 100000
# MAGIC ORDER BY 
# MAGIC     avg_km_driven DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cardekho_cleaned where car_age <= 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select manufacturer_of_car,model,count(*) as total from cardekho_cleaned where total_km_driven >= 100000 and car_age >= 5 group by model,manufacturer_of_car order by total desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select manufacturer_of_car,model,count(*) as total from cardekho_cleaned where total_km_driven between 50000 and 100000 group by model,manufacturer_of_car order by total desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select manufacturer_of_car,model,count(*) as total from cardekho_cleaned where total_km_driven >= 50000 group by model,manufacturer_of_car order by total desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select manufacturer_of_car,model,count(*) as total from cardekho_cleaned group by model,manufacturer_of_car order by total desc;