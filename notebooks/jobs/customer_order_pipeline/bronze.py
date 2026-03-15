# customers.json

# json
# {"customer_id":1,"name":"John","city":"New York","update_ts":"2024-01-01T10:00:00"}
# {"customer_id":2,"name":"Alice","city":"Chicago","update_ts":"2024-01-01T11:00:00"}
# {"customer_id":3,"name":"Bob","city":"Dallas","update_ts":"2024-01-01T12:00:00"}
# {"customer_id":2,"name":"Alice","city":"Boston","update_ts":"2024-01-02T09:00:00"}

# orders1.json
# {"order_id":101,"customer_id":1,"amount":100,"status":"created","order_time":"2024-01-01T10:10:00"}
# {"order_id":102,"customer_id":2,"amount":200,"status":"created","order_time":"2024-01-01T11:15:00"}

# orders2.json
# {"order_id":103,"customer_id":3,"amount":150,"status":"shipped","order_time":"2024-01-02T12:00:00"}
# {"order_id":104,"customer_id":1,"amount":300,"status":"delivered","order_time":"2024-01-02T13:00:00"}


## imports

import dlt
import spark
from pyspark.sql.functions import *

# 3. Customer Bronze

@dlt.table(
  name="customer_bronze",
  comment="Raw customer data"
)
def customer_bronze():

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","json")
        .load("/FileStore/data/customers/")
    )

# Customer SCD Type 1

dlt.create_streaming_table("customer_scd1_bronze")

dlt.apply_changes(
    target = "customer_scd1_bronze",
    source = "customer_bronze",
    keys = ["customer_id"],
    sequence_by = col("update_ts"),
    stored_as_scd_type = 1
)

# 5. Customer SCD Type 2

dlt.create_streaming_table("customer_scd2_bronze")

dlt.apply_changes(
    target = "customer_scd2_bronze",
    source = "customer_bronze",
    keys = ["customer_id"],
    sequence_by = col("update_ts"),
    stored_as_scd_type = 2
)

# 6. Orders Auto Loader

@dlt.table(
  name="orders_autoloader_bronze"
)
def orders_autoloader_bronze():

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format","json")
        .load("/FileStore/data/orders/")
    )

# 7. Orders Bronze

@dlt.table(
  name="orders_bronze"
)
def orders_bronze():

    df = dlt.read_stream("orders_autoloader_bronze")

    return df.withColumn("ingest_time", current_timestamp())

# 8. Orders Union Bronze

@dlt.table(
  name="orders_union_bronze"
)
def orders_union_bronze():

    auto_loader = dlt.read_stream("orders_autoloader_bronze")
    bronze = dlt.read_stream("orders_bronze")

    return auto_loader.unionByName(bronze)

# 9. Joined View

@dlt.view(
  name="joined_vw"
)
def joined_vw():

    orders = dlt.read("orders_union_bronze")
    customers = dlt.read("customer_scd1_bronze")

    return (
        orders.join(customers,"customer_id","left")
    )

# 10. Orders Silver

@dlt.table(
  name="orders_silver"
)
def orders_silver():

    df = dlt.read("joined_vw")

    return df.filter("order_id IS NOT NULL")

# 11. Gold Aggregation — by Customer

@dlt.table(
 name="orders_agg_c_gold"
)
def orders_agg_c_gold():

    df = dlt.read("orders_silver")

    return (
        df.groupBy("customer_id","name")
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("total_revenue")
        )
    )

# 12. Gold Aggregation — by Status

@dlt.table(
 name="orders_agg_s_gold"
)
def orders_agg_s_gold():

    df = dlt.read("orders_silver")

    return (
        df.groupBy("status")
        .agg(
            count("order_id").alias("orders"),
            sum("amount").alias("revenue")
        )
    )

# 13. Gold Aggregation — Overall

@dlt.table(
 name="orders_agg_o_gold"
)
def orders_agg_o_gold():

    df = dlt.read("orders_silver")

    return (
        df.agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("total_revenue")
        )
    )