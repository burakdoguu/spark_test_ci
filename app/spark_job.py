from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def spark_process():
    spark = SparkSession \
     .builder \
     .appName("Spark Demo") \
     .getOrCreate() 

    raw_df = spark.read \
        .format("json") \
        .option("path", "/opt/bitnami/spark/app/Invoice-set3.json") \
        .option("cleanSource", "delete") \
        .load()

    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
                               "CustomerType", "PaymentMethod", "DeliveryType",
                               "DeliveryAddress.City", "DeliveryAddress.State",
                               "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    query_df = flattened_df.select("InvoiceNumber", "ItemCode", "ItemPrice", "ItemQty", "TotalValue", "CreatedTime",
                                   "StoreID", "PaymentMethod", "City")
    
    query_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/postgres") \
        .option("dbtable", "InvoicesTable") \
        .option("user", "postgres")\
        .option("password", "postgres")\
        .save()