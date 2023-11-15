
class BatchProcess:
    def __init__(self) -> None:
        self.data_dir = "/opt/bitnami/spark/app/Invoice-set3.json"
    
    def spark_process(self):
        from pyspark.sql import SparkSession
        spark = SparkSession \
                .builder \
                .appName("Spark Demo") \
                .getOrCreate() 

        return spark

    def readData(self,spark):
        raw_data = spark.read \
            .format("json") \
            .option("path", f"{self.data_dir}") \
            .load()
                
        return raw_data

    def explode_to_flattened(self, df):
        from pyspark.sql.functions import expr
        explode_df = df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID",
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

        return flattened_df

    def create_query_df(self,flattened):
        query_df = flattened.select("InvoiceNumber", "ItemCode", "ItemPrice", "ItemQty", "TotalValue", "CreatedTime",
                                   "StoreID", "PaymentMethod", "City")

        return query_df

    def writeData(self,final_df):
        write_final_df = final_df.write.format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/postgres") \
            .option("dbtable", "InvoicesTable") \
            .option("user", "postgres")\
            .option("password", "postgres")
        
        return write_final_df

    def process(self):
        print("Starting Processing....",end='')
        spark = self.spark_process()
        df = self.readData(spark=spark)
        flattened = self.explode_to_flattened(df)
        query = self.create_query_df(flattened=flattened)
        writeProcess = self.writeData(final_df=query)
        print('Batch Processing Done...\n')
        return writeProcess
    



btch = BatchProcess()
btch.process()
        