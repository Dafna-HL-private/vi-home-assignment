from pyspark.sql.functions import col, avg


def most_traded_stock(df):

    # trading value
    df = df.withColumn("trading_value", col("close") * col("volume"))

    # average trading value for each stock
    avg_trading_value = df.groupBy("ticker").agg(avg("trading_value").alias("frequency")).\
        orderBy(col("frequency").desc()).limit(1)

    avg_trading_value.show()



