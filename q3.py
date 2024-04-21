from pyspark.sql.functions import col


def most_volatile_stock(df, spark):

    # daily return
    df = df.withColumn("daily_return", (col("close") - col("open")) / col("open"))
    df.createOrReplaceTempView("stock_data")

    query = """
        SELECT ticker,
               SQRT(STDDEV(daily_return) * 252) AS annualized_std_dev
        FROM (
            SELECT ticker,
                   daily_return
            FROM stock_data
        )
        GROUP BY ticker
        ORDER BY annualized_std_dev DESC
        LIMIT 1
    """

    result = spark.sql(query)
    result.show()

