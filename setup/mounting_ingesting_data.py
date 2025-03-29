# Databricks notebook source
dbServer = 'cardekho-server'
dbPort = '1433'
dbName = 'cardekho-db'
dbUser = 'krishna1303'
dbPassword = 'cardekhosql-password'
databricksScope = 'databricks-scopeforcardekho'

# COMMAND ----------

connectionUrl ='jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(dbServer, dbPort,dbName, dbUser)
dbPassword = dbutils.secrets.get(scope = databricksScope, key='cardekhosql-password')
connectionProperties = {
'password': dbPassword,
'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}

# COMMAND ----------

validStatusDf = spark.read.jdbc(url = connectionUrl, table = 'dbo.cardekho_columns', properties= connectionProperties )

# COMMAND ----------

validStatusDf.show()

# COMMAND ----------

expected_columns = validStatusDf.count()

# COMMAND ----------

# MAGIC %fs ls /mnt/cardekho-landing

# COMMAND ----------

from pyspark.sql.functions import col

# Define paths
landing_path = "/mnt/cardekho-landing/"
bronze_layer_path = "/mnt/cardekho-raw/cardekho_bronze_layer/cardekho"
bad_data_path = "/mnt/bad-data/"

# Mount the storage account if not already mounted
if not any(mount.mountPoint == landing_path for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount('/mnt/cardekho-landing')
dbutils.fs.mount(
    source='wasbs://cardekho-landing@cardekhoblob.blob.core.windows.net',
    mount_point='/mnt/cardekho-landing',
    extra_configs={
        'fs.azure.account.key.cardekhoblob.blob.core.windows.net':dbutils.secrets.get(scope = "databricks-scopeforcardekho", key = "cardekhokey")}
    )

# List all files in the landing zone
files = dbutils.fs.ls(landing_path)

# Process each file
for file in files:
    file_path = file.path
    print(f"Processing file: {file_path}")

    # Read file as DataFrame
    try:
        cardekho_raw_df = spark.read.option("header", True).csv(file_path)

        # Check if the number of columns matches
        if len(cardekho_raw_df.columns) == expected_columns:
            dbutils.fs.mv(file_path, bronze_layer_path)
            print(f" {file.name} moved successfully to Bronze Layer.")
        else:
            dbutils.fs.mv(file_path, bad_data_path)
            print(f" {file.name} moved to Bad Data folder (Column mismatch).")
            dbutils.notebook.exit(1)

    except Exception as e:
        print(f"Error processing {file.name}: {str(e)}")
        dbutils.fs.mv(file_path, bad_data_path)  # Move to bad data if there's a read error

print("Data ingesting into bronze layer completed")