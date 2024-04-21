from pyspark.sql.functions import col, avg


def average_daily_return(df):

    # daily return
    df = df.withColumn("daily_return", (col("close") - col("open")) / col("open"))

    # avg daily return
    avg_daily_return = df.groupBy("date").agg(avg("daily_return").alias("average_return"))

    avg_daily_return.show()



