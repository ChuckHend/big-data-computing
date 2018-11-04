/* 
which stock has the highest average daily price change?
 for a day-trading strategy, we want stocks that move a lot during a single day as a percentage of their price
 so, we can look at the average difference between the daily high/low as a percentage of the opening price
 Stocks with higher avg_movement have more price movements. these are candidates for day trading strategies
 Conversely, stocks that have prices that dont move will not make money on day trades
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val raw = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/stocks")

// calculate the daily summary table
// this is simply the values at the close, ie the last data point for each day
// some stocks have fragmented data, so last data point could be at noon, for exmaple.
// append _s to annotate "summary"
val day_rk = Window.partitionBy("date", "ticker").orderBy(col("time").desc)
val summary = raw
    .withColumn("day_row_rank", rank().over(day_rk))
    .filter("day_row_rank == 1")
    .select(col("open").as("open_s"), col("high").as("high_s"), col("low").as("low_s"), col("close").as("close_s"), col("ticker"), col("date"))

val min_max_price = Window.partitionBy("ticker","date")
// calculate the difference between the spot prices for min and max each day
// divide by the opening price for the day to get a "percentage change"
// calculate the average percentage change over each day
raw
    .withColumn("price_diff",abs(max(col("open")).over(min_max_price) - min(col("open")).over(min_max_price)))
    .drop("open")
    .join(summary, Seq("ticker","date"))
    .withColumn("price_diff_perc", col("price_diff") / col("open_s"))
    .select("ticker","date","price_diff","price_diff_perc")
    .distinct()
    .groupBy("ticker").agg(avg(col("price_diff_perc")).as("avg_movement"))
    .orderBy(col("avg_movement").desc)
    .show()

