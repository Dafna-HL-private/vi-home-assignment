from pyspark.sql import SparkSession
from pyspark.sql.functions import last, coalesce, to_date
from pyspark.sql.window import Window

from q1 import average_daily_return
from q2 import most_traded_stock
from q3 import most_volatile_stock
from q4 import top_3_30_days_return


if __name__ == '__main__':

    spark = SparkSession.builder.appName("vi assignment") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # csv to df
    df = spark.read.csv("stock_prices.csv", header=True, inferSchema=True)
    df = df.withColumn("date", to_date(df.date, "MM/dd/yyyy"))

    # ------ fill missing data
    windowSpecBackward = Window.partitionBy("ticker").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

    filled_open = last(df['open'], ignorenulls=True).over(windowSpecBackward)
    filled_high = last(df['high'], ignorenulls=True).over(windowSpecBackward)
    filled_low = last(df['low'], ignorenulls=True).over(windowSpecBackward)
    filled_close = last(df['close'], ignorenulls=True).over(windowSpecBackward)
    filled_volume = last(df['volume'], ignorenulls=True).over(windowSpecBackward)

    df = df.withColumn("open", coalesce(df["open"], filled_open))
    df = df.withColumn("high", coalesce(df["high"], filled_high))
    df = df.withColumn("low", coalesce(df["low"], filled_low))
    df = df.withColumn("close", coalesce(df["close"], filled_close))
    df = df.withColumn("volume", coalesce(df["volume"], filled_volume))

    # ----------------------------- Q1 -----------------------------

    print("q1 answer:")
    average_daily_return(df)

    # ----------------------------- Q2 -----------------------------

    print("q2 answer:")
    most_traded_stock(df)

    # ----------------------------- Q3 -----------------------------

    print("q3 answer:")
    most_volatile_stock(df, spark)

    # ----------------------------- Q4 -----------------------------

    print("q4 answer:")
    top_3_30_days_return(df, spark)

    spark.stop()

