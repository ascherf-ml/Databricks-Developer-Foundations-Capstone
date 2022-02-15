# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #2 - Batch Ingestion
# MAGIC 
# MAGIC In this exercise you will be ingesting three batches of orders, one for 2017, 2018 and 2019.
# MAGIC 
# MAGIC As each batch is ingested, we are going to append it to a new Delta table, unifying all the datasets into one single dataset.
# MAGIC 
# MAGIC Each year, different individuals and different standards were used resulting in datasets that vary slightly:
# MAGIC * In 2017 the backup was written as fixed-width text files
# MAGIC * In 2018 the backup was written a tab-separated text files
# MAGIC * In 2019 the backup was written as a "standard" comma-separted text files but the format of the column names was changed
# MAGIC 
# MAGIC Our only goal here is to unify all the datasets while tracking the source of each record (ingested file name and ingested timestamp) should additional problems arise.
# MAGIC 
# MAGIC Because we are only concerned with ingestion at this stage, the majority of the columns will be ingested as simple strings and in future exercises we will address this issue (and others) with various transformations.
# MAGIC 
# MAGIC As you progress, several "reality checks" will be provided to you help ensure that you are on track - simply run the corresponding command after implementing the corresponding solution.
# MAGIC 
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 2.A - Ingest Fixed-Width File
# MAGIC * Exercise 2.B - Ingest Tab-Separated File
# MAGIC * Exercise 2.C - Ingest Comma-Separated File

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #2</h2>
# MAGIC 
# MAGIC To get started, run the following cell to setup this exercise, declaring exercise-specific variables and functions.

# COMMAND ----------

# MAGIC %run ./_includes/Setup-Exercise-02

# COMMAND ----------

# MAGIC %md Run the following cell to preview a list of the files you will be processing in this exercise.

# COMMAND ----------

files = dbutils.fs.ls(f"{working_dir}/raw/orders/batch") # List all the files
display(files)                                           # Display the list of files

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.A - Ingest Fixed-Width File</h2>
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2017_path`**, and **`dbutils.fs.head`** to investigate the 2017 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the text file identified by **`batch_2017_path`** - this should provide one record per line, with a single column named **`value`**
# MAGIC 3. Using the information in **`fixed_width_column_defs`** (or the dictionary itself) use the **`value`** column to extract each new column of the appropriate length.<br/>
# MAGIC   * The dictionary's key is the column name
# MAGIC   * The first element in the dictionary's value is the starting position of that column's data
# MAGIC   * The second element in the dictionary's value is the length of that column's data
# MAGIC 4. Once you are done with the **`value`** column, remove it.
# MAGIC 5. For each new column created in step #3, remove any leading whitespace
# MAGIC   * The introduction of \[leading\] white space should be expected when extracting fixed-width values out of the **`value`** column.
# MAGIC 6. For each new column created in step #3, replace all empty strings with **`null`**.
# MAGIC   * After trimming white space, any column for which a value was not specified in the original dataset should result in an empty string.
# MAGIC 7. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from.
# MAGIC   * This should not be hard coded.
# MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
# MAGIC 8. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame.
# MAGIC   * This should not be hard coded.
# MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
# MAGIC 9. Write the corresponding **`DataFrame`** in the "delta" format to the location specified by **`batch_target_path`**
# MAGIC 
# MAGIC **Special Notes:**
# MAGIC * It is possible to use the dictionary **`fixed_width_column_defs`** and programatically extract <br/>
# MAGIC   each column but, it is also perfectly OK to hard code this step and extract one column at a time.
# MAGIC * The **`SparkSession`** is already provided to you as an instance of **`spark`**.
# MAGIC * The classes/methods that you will need for this exercise include:
# MAGIC   * **`pyspark.sql.DataFrameReader`** to ingest data
# MAGIC   * **`pyspark.sql.DataFrameWriter`** to ingest data
# MAGIC   * **`pyspark.sql.Column`** to transform data
# MAGIC   * Various functions from the **`pyspark.sql.functions`** module
# MAGIC   * Various transformations and actions from **`pyspark.sql.DataFrame`**
# MAGIC * The following methods can be used to investigate and manipulate the Databricks File System (DBFS)
# MAGIC   * **`dbutils.fs.ls(..)`** for listing files
# MAGIC   * **`dbutils.fs.rm(..)`** for removing files
# MAGIC   * **`dbutils.fs.head(..)`** to view the first N bytes of a file
# MAGIC 
# MAGIC **Additional Requirements:**
# MAGIC * The unified batch dataset must be written to disk in the "delta" format
# MAGIC * The schema for the unified batch dataset must be:
# MAGIC   * **`submitted_at`**:**`string`**
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`customer_id`**:**`string`**
# MAGIC   * **`sales_rep_id`**:**`string`**
# MAGIC   * **`sales_rep_ssn`**:**`string`**
# MAGIC   * **`sales_rep_first_name`**:**`string`**
# MAGIC   * **`sales_rep_last_name`**:**`string`**
# MAGIC   * **`sales_rep_address`**:**`string`**
# MAGIC   * **`sales_rep_city`**:**`string`**
# MAGIC   * **`sales_rep_state`**:**`string`**
# MAGIC   * **`sales_rep_zip`**:**`string`**
# MAGIC   * **`shipping_address_attention`**:**`string`**
# MAGIC   * **`shipping_address_address`**:**`string`**
# MAGIC   * **`shipping_address_city`**:**`string`**
# MAGIC   * **`shipping_address_state`**:**`string`**
# MAGIC   * **`shipping_address_zip`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`string`**
# MAGIC   * **`product_sold_price`**:**`string`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

# MAGIC %md ### Fixed-Width Meta Data 
# MAGIC 
# MAGIC The following dictionary is provided for reference and/or implementation<br/>
# MAGIC (depending on which strategy you choose to employ).
# MAGIC 
# MAGIC Run the following cell to instantiate it.

# COMMAND ----------

fixed_width_column_defs = {
  "submitted_at": (1, 15),
  "order_id": (16, 40),
  "customer_id": (56, 40),
  "sales_rep_id": (96, 40),
  "sales_rep_ssn": (136, 15),
  "sales_rep_first_name": (151, 15),
  "sales_rep_last_name": (166, 15),
  "sales_rep_address": (181, 40),
  "sales_rep_city": (221, 20),
  "sales_rep_state": (241, 2),
  "sales_rep_zip": (243, 5),
  "shipping_address_attention": (248, 30),
  "shipping_address_address": (278, 40),
  "shipping_address_city": (318, 20),
  "shipping_address_state": (338, 2),
  "shipping_address_zip": (340, 5),
  "product_id": (345, 40),
  "product_quantity": (385, 5),
  "product_sold_price": (390, 20)
}

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.A
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dbutils.fs.rm(batch_target_path, True)

# COMMAND ----------

df = spark.read.format("text").load(batch_2017_path)

# COMMAND ----------

# split value into columns
for key, value in fixed_width_column_defs.items():
    df = df.withColumn(key, F.substring("value", value[0], value[1]))
    
df_split = df.drop("value")
#df_split.display()

# COMMAND ----------

# Remove leading spaces in columns
df_nolead = df_split
for col in df_split.columns:
  df_nolead=df_nolead.withColumn(col, F.trim(col))
#df_nolead.display()

# COMMAND ----------

# change empty string to null
df_nonull=df_nolead.select([F.when(F.col(c)=="",None).otherwise(F.col(c)).alias(c) for c in df_nolead.columns])

# COMMAND ----------

# ingest filename and timestamp
df_filenametime = df_nonull
df_filenametime = df_filenametime.withColumn("ingest_file_name", F.input_file_name())\
                         .withColumn("ingested_at", F.current_timestamp())
#df_filenametime.display()

# COMMAND ----------

df_filenametime.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(batch_target_path)

# COMMAND ----------

df_filenametime.count()

# COMMAND ----------

# MAGIC %md ### Reality Check #2.A
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_a()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.B - Ingest Tab-Separted File</h2>
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2018_path`**, and **`dbutils.fs.head`** to investigate the 2018 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the tab-separated file identified by **`batch_2018_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created datasets specified by **`batch_target_path`**
# MAGIC 
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.b
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

df2 = spark.read.format("csv")\
                 .option("sep", "\t")\
                 .option("header", True)\
                 .load(batch_2018_path)

df2_filenametime = df2.withColumn("ingest_file_name", F.input_file_name()).withColumn("ingested_at", F.current_timestamp())

df2_nonull = df2_filenametime
for col in df2_nonull.columns:
  df2_nonull=df2_nonull.withColumn(col, F.expr(f"case when {col}='null' then null else {col} end"))

#df2_nonull.display()

# COMMAND ----------

df2_nonull.count()

# COMMAND ----------

df2_nonull.write.format("delta").mode("append").save(batch_target_path)

# COMMAND ----------

# MAGIC %md ### Reality Check #2.B
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_b()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.C - Ingest Comma-Separted File</h2>
# MAGIC 
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2019_path`**, and **`dbutils.fs.head`** to investigate the 2019 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the comma-separated file identified by **`batch_2019_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created dataset specified by **`batch_target_path`**<br/>
# MAGIC    Note: The column names in this dataset must be updated to conform to the schema defined for Exercise #2.A - there are several strategies for this:
# MAGIC    * Provide a schema that alters the names upon ingestion
# MAGIC    * Manually rename one column at a time
# MAGIC    * Use **`fixed_width_column_defs`** programaticly rename one column at a time
# MAGIC    * Use transformations found in the **`DataFrame`** class to rename all columns in one operation
# MAGIC 
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**<br/>

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.C
# MAGIC 
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

df3 = spark.read.format("csv")\
                .option("header", True)\
                .schema(df2.schema)\
                .load(batch_2019_path)

df3_nonull = df3
for col in df3_nonull.columns:
  df3_nonull=df3_nonull.withColumn(col, F.expr(f"case when {col}='null' then null else {col} end"))
#df3_nonull.display()

# COMMAND ----------

df3_nonull.write.format("delta").mode("append").save(batch_target_path)

# COMMAND ----------

# MAGIC %md ### Reality Check #2.C
# MAGIC Run the following command to ensure that you are on track:

# COMMAND ----------

reality_check_02_c()

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2 - Final Check</h2>
# MAGIC 
# MAGIC Run the following command to make sure this exercise is complete:

# COMMAND ----------

reality_check_02_final()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>