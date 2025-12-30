# Databricks notebook source
# MAGIC %md
# MAGIC > ## **Bronze Layer**

# COMMAND ----------

## Create a list of all the tables in the silver 
layer_up_to_silver = ['customers', 
                      'geolocation', 
                      'orders', 
                      'order_items', 
                      'order_payments', 
                      'order_reviews', 
                      'product', 
                      'sellers', 
                      'product_category_name_translation']

# COMMAND ----------

for layer in layer_up_to_silver:

    ## Load csv file as a dataframe
    df_start = spark.read.format('csv')\
        .option('header', 'true')\
        .option('inferSchema', 'true')\
        .load(f'/Volumes/ae_main/sources/olist_sources/{layer}')

    ## Get schema
    df_schema = df_start.schema

    ## Read stream as dataframe
    df = spark.readStream.format('csv')\
        .option('header', 'true')\
        .schema(df_schema)\
        .load(f'/Volumes/ae_main/sources/olist_sources/{layer}')          

    ## Write stream as delta table
    df.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', f'/Volumes/ae_main/bronze_layer/checkpoint/{layer}')\
        .trigger(once=True)\
        .toTable(f'ae_main.bronze_layer.{layer}')
        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Review Case Column Shift

# COMMAND ----------

df_bronze_orders_reviews = (
    spark.read
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", True)
    .option("ignoreLeadingWhiteSpace", True)
    .option("ignoreTrailingWhiteSpace", True)
    .csv("/Volumes/ae_main/sources/olist_sources/order_reviews/order_reviews.csv")
)


# COMMAND ----------

df_bronze_orders_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ae_main.bronze_layer.order_reviews")
