from pyspark.sql.functions import min, max, col, date_add, lit
from pyspark.sql.types import IntegerType


def top_3_30_days_return(df, spark):

    # ------ fill missing dates

    # create dates from start to end df
    date_range = df.select(min("date").alias("start_date"), max("date").alias("end_date")).collect()[0]
    start_date = date_range.start_date
    end_date = date_range.end_date
    days_count = (end_date - start_date).days + 1
    dates = spark.range(days_count).select(date_add(lit(start_date), col("id").cast(IntegerType())).alias("date"))

    # dates.show()

    # create df with all dates for ticker - with the closest close data
    df.createOrReplaceTempView("stock_prices")
    dates.createOrReplaceTempView("dates")

    query = """
            with corss_join_data as
            (select *
            from 
            (select distinct ticker from stock_prices) a
            cross join (select distinct date from dates) b
            )
            
            ,all_dates_data as
            (select a.*,
                    b.open,
                    b.high,
                    b.low,
                    b.close,
                    b.volume
            from corss_join_data a
            left join stock_prices b
            on a.ticker = b.ticker
            and a.date = b.date)
            
            ,complete_data_1 as
            (select *,
                    count(close) over(partition by ticker order by date) as grouper
            from all_dates_data
            )
            
            ,complete_data_2 as
            (select ticker,
                    date,
                    open,
                    high,
                    low,
                    close,
                    max(close) over(partition by ticker,grouper) as close_forward_filled,
                    volume
            from complete_data_1)
            
            select ticker, date, open, high, low, close_forward_filled as close, volume
            from complete_data_2
            order by ticker, date     
    """

    df = spark.sql(query)
    # df.show()

    # ------ calculate top 3 - 30 days return
    df.createOrReplaceTempView("stock_prices")

    query = """
    WITH Returns AS (
        SELECT
            ticker,
            date,
            close,
            LAG(close, 30) OVER (PARTITION BY ticker ORDER BY date) AS close_30_days_ago,
            ((close - LAG(close, 30) OVER (PARTITION BY ticker ORDER BY date)) / LAG(close, 30) OVER (PARTITION BY ticker ORDER BY date) * 100) AS return_pct
        FROM
            stock_prices
    )
    , RankedReturns AS (
        SELECT
            ticker,
            date,
            return_pct,
            ROW_NUMBER() OVER (ORDER BY return_pct DESC) AS rank
        FROM
            Returns
        WHERE
            return_pct IS NOT NULL
    )
    SELECT
        ticker,
        date,
        return_pct
    FROM
        RankedReturns
    WHERE
        rank <= 3
    """

    top_three_returns = spark.sql(query)
    top_three_returns.show()


