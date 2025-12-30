# Databricks notebook source
# MAGIC %md
# MAGIC > ## **Silver Layer** ##

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers Table

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from typing import List
from pyspark.sql.functions import col, count, countDistinct, when, lpad, initcap, upper, trim, to_timestamp, lit, date_format, round, coalesce, length, expr, udf, encode, decode
from pyspark.sql.types import StringType, TimestampType, IntegerType
from datetime import datetime
import os
import sys

# COMMAND ----------

def exact_duplicates(df: DataFrame, all_cols: List[str]) -> DataFrame:
    """Returns a Spark DataFrame with the exact duplicates in the bronze layer."""
    cols = all_cols or df.columns
    return (df.groupBy(*cols) \
        .count() \
        .filter(col("count") > 1))

# display(exact_duplicates)


# COMMAND ----------

def is_null(df: DataFrame, all_cols: List[str]) -> DataFrame:
    """Returns a Spark DataFrame with the number of null values in each column."""
    cols = all_cols or df.columns
    null_values = [
        count(when(col(c).isNull(), 1)).alias(c) 
        for c in df.columns
    ]
    return df.select(null_values)
# df_cus.select(is_null_cols).display()

# COMMAND ----------

df_cus = spark.read.table('ae_main.bronze_layer.customers')
display(df_cus.limit(5))

# COMMAND ----------

x = exact_duplicates(df_cus, df_cus.columns)
display(x)

# COMMAND ----------

y = is_null(df_cus, df_cus.columns)
display(y)

# COMMAND ----------

# Column names to change and add prefix to zip code
df_cus_silver = df_cus\
                .withColumn("zip_code",lpad(col("customer_zip_code_prefix").cast(StringType()), 5, "0")) \
                .withColumn("city",initcap(trim(col("customer_city")))) \
                .withColumn("state",upper(trim(col("customer_state")))) \
                .withColumnRenamed("customer_unique_id", "unique_customer_id")\
                .drop("customer_zip_code_prefix", "customer_city", "customer_state", "customer_unique_id")

#display(df_cus_silver)



# COMMAND ----------

df_cus_silver.orderBy("city")\
    .write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Orders Table

# COMMAND ----------

df_ord = spark.read.table("ae_main.bronze_layer.orders")
ord_d = exact_duplicates(df_ord, df_ord.columns)
ord_n = is_null(df_ord, df_ord.columns)
display(df_ord.limit(5))
# display(ord_d)
# display(ord_n)

# COMMAND ----------

df_ord = (df_ord
        .withColumn("order_purchase_timestamp",try_to_timestamp(col("order_purchase_timestamp"),lit("d/M/yyyy H:m")))
        .withColumn("order_approved_at",try_to_timestamp(col("order_approved_at"),lit("d/M/yyyy H:m")))
        .withColumn("order_delivered_carrier_date",try_to_timestamp(col("order_delivered_carrier_date"),lit("d/M/yyyy H:m")))
        .withColumn("order_delivered_customer_date",try_to_timestamp(col("order_delivered_customer_date"),lit("d/M/yyyy H:m")))
        .withColumn("order_estimated_delivery_date",try_to_timestamp(col("order_estimated_delivery_date"),lit("d/M/yyyy H:m")))
        .withColumn("order_status",initcap(trim(col("order_status"))))
        )

display(df_ord.limit(5))

# COMMAND ----------

df_ord = (df_ord
        .withColumnRenamed("order_purchase_timestamp", "purchase_at")
        .withColumnRenamed("order_approved_at", "approved_at")
        .withColumnRenamed("order_delivered_carrier_date", "shipped_at")
        .withColumnRenamed("order_delivered_customer_date", "delivered_at")
        .withColumnRenamed("order_estimated_delivery_date", "estimated_delivery_at")
        )

# COMMAND ----------

df_ord = (df_ord.withColumn("delivery_phase", 
    when(col("order_status") == "Delivered", "Completed")
    .when(col("order_status") == "Shipped", "In Transit")
    .when(col("order_status").isin("Canceled", "Unavailable"), "Unsuccessful")
    .otherwise("Processing"))
          )

# COMMAND ----------

display(df_ord)

# COMMAND ----------

df_ord.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Order Items Table

# COMMAND ----------

df_oit = spark.read.table("ae_main.bronze_layer.order_items")
oit_d = exact_duplicates(df_oit, df_oit.columns)
oit_n = is_null(df_oit, df_oit.columns)
display(df_oit.limit(5))
display(oit_d)
display(oit_n)

# COMMAND ----------

df_oit= (
    df_oit
    .withColumnRenamed("shipping_limit_date", "shipping_deadline")
    .withColumnRenamed("freight_value", "shipping_fee")
    )
# display(df_oit)

# COMMAND ----------

df_oit = (df_oit
        .withColumn("price", col("price").cast("decimal(10,2)"))
        .withColumn("shipping_fee", col("shipping_fee").cast("decimal(10,2)"))
)

df_oit = (df_oit
        .withColumn("total_amount", round(col("price") + col("shipping_fee"),2)))

df_oit = df_oit.orderBy("shipping_deadline")
display(df_oit.show(5))

# COMMAND ----------

df_oit.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Table

# COMMAND ----------

df_pro = spark.read.table('ae_main.bronze_layer.product')
pro_d = exact_duplicates(df_pro, df_pro.columns)
pro_n = is_null(df_pro, df_pro.columns)
display(df_pro.limit(5))
display(pro_d)
display(pro_n)

# COMMAND ----------

df_pro = (df_pro
    .withColumnRenamed("product_category_name", "category")
    .withColumnRenamed("product_name_lenght", "name_length")
    .withColumnRenamed("product_description_lenght", "desc_length")
    .withColumnRenamed("product_photos_qty", "photos_qty")
    .withColumnRenamed("product_weight_g", "weight_g")
    .withColumnRenamed("product_length_cm", "length_cm")
    .withColumnRenamed("product_height_cm", "height_cm")
    .withColumnRenamed("product_width_cm", "width_cm"))

# display(df_pro.limit(5))

# COMMAND ----------

df_pro = (df_pro
          .withColumn("category", coalesce(col("category"), lit("Unknown")))
          .withColumn("category", initcap(col("category"))))
#display(df_pro)

# COMMAND ----------

df_pro = (df_pro
    .fillna({
        "name_length" : 0,
        "desc_length" : 0,
        "photos_qty" : 0
    }))
# display(df_pro)

# COMMAND ----------

df_pro.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.product")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review Table

# COMMAND ----------

df_re = spark.read.table("ae_main.bronze_layer.order_reviews")
df_re_d = exact_duplicates(df_re, df_re.columns)
df_re_n = is_null(df_re, df_re.columns)
display(df_re_d)
display(df_re_n)
display(df_re)

# COMMAND ----------

df_re = df_re.fillna({
    "review_comment_title": "No title",
    "review_comment_message": "No message",
    "review_score": 0
})

#display(df_re)

# COMMAND ----------

hex_pattern = "^[a-fA-F0-9]{32}$"
df_re = df_re.filter(col("review_id").rlike(hex_pattern))
df_re = df_re.filter(col("review_score").rlike("^[0-9]+$"))
# df_re.display()

# COMMAND ----------

# นิยมตัดคำนำหน้าซ้ำๆ ออก เช่น review_score -> score
df_re = (df_re
            .withColumnRenamed("review_comment_title", "comment_title")
            .withColumnRenamed("review_score", "score")
            .withColumnRenamed("review_comment_message", "comment") 
            .withColumnRenamed("review_creation_date", "created_at")
            .withColumnRenamed("review_answer_timestamp", "answered_at")
)

df_re = df_re.orderBy("score",ascending=False)
#df_re.display(5)

# COMMAND ----------

df_re.select(
    "created_at",
    "answered_at",
    "comment"
).filter(
    col("created_at").rlike("[A-Za-z]")
).show(truncate=False)

# COMMAND ----------

df_re = df_re.withColumn(
    "rating",
    when(col('score') >= 4,"positive")
    .when(col('score') == 3,"neutral")
    .otherwise("negative")
    )

#display(df_re)

# COMMAND ----------

fmt = "d/M/yyyy H:mm"
df_re = (df_re
        .withColumn("created_at", to_timestamp(col("created_at"), fmt)) 
        .withColumn("answered_at", to_timestamp(col("answered_at"), fmt))
        )


print("--- Check Result ---")
df_re.select('review_id','order_id','score','rating','comment_title','comment', 'created_at', 'answered_at').show(5)


# COMMAND ----------

df_re = df_re.select(
        "review_id",
        "order_id",
        col("score").cast(IntegerType()),
        "rating",
        "comment_title",
        "comment",
        "created_at",
        "answered_at"
    )

# COMMAND ----------

df_re.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.order_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Payments

# COMMAND ----------

df_opm = spark.read.table("ae_main.bronze_layer.order_payments")
df_opm_d = exact_duplicates(df_opm, df_opm.columns)
df_opm_n = is_null(df_opm, df_opm.columns)
display(df_opm.limit(5))
display(df_opm_d)
display(df_opm_n)

# COMMAND ----------

df_opm = (df_opm
    .withColumnRenamed("payment_sequential", "sequential")
    .withColumnRenamed("payment_installments", "installments")
    .withColumnRenamed("payment_value", "amount")
    )

display(df_opm.limit(5))

# COMMAND ----------

df_opm = df_opm.withColumn(
    "payment_method",
    when(col('payment_type') == "credit_card","credit_card")
    .when(col('payment_type') == "debit_card","debit_card")
    .when(col('payment_type') == "boleto","bank_transfer")
    .when(col('payment_type') == "voucher","voucher")
    .otherwise("other"))

# COMMAND ----------

df_opm = (
    df_opm.select(
        "order_id",
        "payment_method",
        col("installments").cast(StringType()),
        "amount"
    ).orderBy("amount", ascending=False)
)
display(df_opm.limit(5))

# COMMAND ----------

df_opm.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.order_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sellers

# COMMAND ----------

df_sel = spark.read.table("ae_main.bronze_layer.sellers")
df_sel_d = exact_duplicates(df_sel, df_sel.columns)
df_sel_n = is_null(df_sel, df_sel.columns)
display(df_sel.limit(5))
display(df_sel_d)
display(df_sel_n)

# COMMAND ----------

df_sel = (df_sel
        .withColumnRenamed("seller_zip_code_prefix", "zip_code")
        .withColumnRenamed("seller_city", "city")
        .withColumnRenamed("seller_state", "state")
        )
    

# COMMAND ----------

df_sel = (df_sel
        .withColumn("zip_code",lpad(col("zip_code").cast(StringType()), 5, "0"))
        .withColumn("city",initcap(trim(col("city"))))
        )

# COMMAND ----------

df_sel = df_sel.select(
        "seller_id",
        "zip_code",
        "city",
        "state"
    ).orderBy("state", ascending=True)

display(df_sel.limit(5))

# COMMAND ----------

df_sel.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.sellers")

# COMMAND ----------

df_geo = spark.read.table("ae_main.bronze_layer.geolocation")
df_geo_d = exact_duplicates(df_geo, df_geo.columns)
df_geo_n = is_null(df_geo, df_geo.columns)
display(df_geo.limit(5))
display(df_geo_d)
display(df_geo_n)

# COMMAND ----------

df_geo = df_geo.withColumn(
    "geolocation_city",
    decode(encode(col("geolocation_city"), "ISO-8859-1"), "UTF-8")
)

# COMMAND ----------

df_geo = df_geo.withColumn(
    "geolocation_city",
    initcap(trim(col("geolocation_city")))
)

# COMMAND ----------

df_geo = (df_geo
        .withColumnRenamed("geolocation_city", "city")
        .withColumnRenamed("geolocation_lat", "lattitude")
        .withColumnRenamed("geolocation_lng", "longitude")
        .withColumnRenamed("geolocation_state", "state")
        ).orderBy("geolocation_zip_code_prefix", ascending=True)

display(df_geo.limit(5))

# COMMAND ----------

df_geo = df_geo.select("geolocation_zip_code_prefix", "city", "lattitude", "longitude", "state").orderBy("geolocation_zip_code_prefix", ascending=True)
df_geo.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable("ae_main.silver_layer.geolocation")