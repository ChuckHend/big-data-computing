/*
Now we can leverage some of the analysis done for question 2, but apply, along with
additional queries it at a per stock level to answer the question:
When should we buy and sell the stocks we highlighted in question 3?
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val raw = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/stocks")

// for each stock, get time of day that the peak and trough happened
val daily_peak = Window.partitionBy("ticker","date").orderBy(col("close").desc)
val daily_trough = Window.partitionBy("ticker","date").orderBy(col("close").asc)
// format a key to designate the hour
// rank each rows price, then flag it as either the peak or the trough
// finally, select only the rows that were peaks or troughs
val peak_trough = raw
    .withColumn("hour", format_string("%04d", col("time")))
    .withColumn("hr", $"hour".substr(lit(0), lit(2)))
    .withColumn("peak", rank.over(daily_peak))
    .withColumn("trough", rank.over(daily_trough))
    .filter("peak==1 or trough==1")

//going to split the peak and trough rows to separate dataframes
//then for each ticker and hour per day, count the number of times that 
//hour of day was a peak or trough

// calc the peaks, high points per day
val peak_w = Window.partitionBy("ticker").orderBy(col("peak_count").desc)
val peaks = peak_trough
    .filter("peak==1")
    .groupBy("ticker","hr").agg(count(col("peak")).as("peak_count"))
    .withColumn("peak_rk", rank().over(peak_w))
    .filter("peak_rk == 1")
    .withColumnRenamed("hr", "peak_hr")
    .select("ticker", "peak_hr")

// calculate the troughs, or low time of day for each stock
val trough_w = Window.partitionBy("ticker").orderBy(col("trough_count").desc)
val troughs = peak_trough
    .filter("trough==1")
    .groupBy("ticker","hr").agg(count(col("trough")).as("trough_count"))
    .withColumn("trough_rk", rank().over(trough_w))
    .filter("trough_rk == 1")
    .withColumnRenamed("hr", "trough_hr")
    .select("ticker", "trough_hr")
// join peaks and troughs together
// result is one row per stock, with columns designating the time of the most frequent peak
// and time of the most frequent trough
val trade_times = peaks.join(troughs, Seq("ticker"))
// a peak is a sell time (or purchase short), and a trough is a buy (or call)

//now, join these times with the output from prob3-3 so we can get time of day for the stock we care about

//START REWORK FROM prob3-3.scala
val minute_rank = Window.partitionBy("ticker","date").orderBy("time")
val std_minRk = Window.partitionBy("ticker","time")
val mean_daily_stdev = Window.partitionBy("ticker")
val predictability = raw
    .withColumn("minute_rank", rank().over(minute_rank))
    .withColumn("stdev_minRk", stddev_pop(col("minute_rank")).over(std_minRk))
    .withColumn("daily_mean_stdev", avg(col("stdev_minRk")).over(mean_daily_stdev))
    .groupBy("ticker").agg(round(avg("daily_mean_stdev"),3).as("mean_stdev"))
    .select("ticker", "mean_stdev")
    .orderBy(col("mean_stdev").asc)
val day_rk = Window.partitionBy("date", "ticker").orderBy(col("time").desc)
val summary = raw
    .withColumn("day_row_rank", rank().over(day_rk))
    .filter("day_row_rank == 1")
    .select(col("open").as("open_s"), col("high").as("high_s"), col("low").as("low_s"), col("close").as("close_s"), col("ticker"), col("date"))
val min_max_price = Window.partitionBy("ticker","date")
val volatility = raw
    .withColumn("price_diff",abs(max(col("open")).over(min_max_price) - min(col("open")).over(min_max_price)))
    .drop("open")
    .join(summary, Seq("ticker","date"))
    .withColumn("price_diff_perc", col("price_diff") / col("open_s"))
    .select("ticker","date","price_diff","price_diff_perc")
    .distinct()
    .groupBy("ticker").agg(round(avg("price_diff_perc"),3).as("avg_perc_movement"))
val latest_price_wind = Window.partitionBy("ticker").orderBy(col("date").desc)
val latest_price = summary
    .withColumn("row_rank", rank().over(latest_price_wind))
    .filter("row_rank == 1")
    .select("ticker", "close_s")
val stocks = predictability
    .join(volatility, Seq("ticker"))
    .filter("avg_perc_movement >= 0.01 and mean_stdev > 0 and mean_stdev <= 0.05")
    .join(latest_price, Seq("ticker"))
//END REWORK FROM prob3-3.scala
 
/* 
join this to trade_times 
 the first stock in this list is the answer to prob3-3
 this list, in decreasing order, gives us the times of day we should buy and sell
 each stocks
*/
val results = stocks.join(trade_times, Seq("ticker"))
    .orderBy(col("avg_perc_movement").desc)