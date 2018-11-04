/*
which stock has a predictable price movement?
 it can be useful to identify stocks that not only have large daily price movements
 but also that those daily price movements are similiar day-to-day
*/
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// the stock with the lowest standard deviation follows a pattern most
//    similar to the prior days pattern, on a by minute basis

// load data
val raw = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/stocks")

// for each stock and day, calculate the rank of each minutes price
val minute_rank = Window.partitionBy("ticker","date").orderBy("time")
// for each (stock, time of day), calculate the standard deviation of those ranks
val std_minRk = Window.partitionBy("ticker","time")

// for each (stock), calculate the avg std for all days
val mean_daily_stdev = Window.partitionBy("ticker")

// apply the window functions to the dataframe
val predictability = raw
    .withColumn("minute_rank", rank().over(minute_rank))
    .withColumn("stdev_minRk", stddev_pop(col("minute_rank")).over(std_minRk))
    .withColumn("daily_mean_stdev", avg(col("stdev_minRk")).over(mean_daily_stdev))
    .groupBy("ticker").agg(round(avg("daily_mean_stdev"),3).as("mean_stdev"))
    .select("ticker", "mean_stdev")
    .orderBy(col("mean_stdev").asc)

// now join that output to the table we create for problem 1
// this will give us an indication of stocks that have predictible volatility (not just predictible movements)
val day_rk = Window.partitionBy("date", "ticker").orderBy(col("time").desc)
val summary = raw
    .withColumn("day_row_rank", rank().over(day_rk))
    .filter("day_row_rank == 1")
    .select(col("open").as("open_s"), col("high").as("high_s"), col("low").as("low_s"), col("close").as("close_s"), col("ticker"), col("date"))

/* for each ticker and day, we can compute the difference between max and min price
  for example, we could buy at the low point and sell at the high point from one day to another
*/
val min_max_price = Window.partitionBy("ticker","date")

/* apply the window function
 then join to summary so we can divide by the price of the stack at the beginning of the day
 this will somewhat normalize the stocks by showing the volatility as a percentage of its price
*/
val volatility = raw
    .withColumn("price_diff",abs(max(col("open")).over(min_max_price) - min(col("open")).over(min_max_price)))
    .drop("open")
    .join(summary, Seq("ticker","date"))
    .withColumn("price_diff_perc", col("price_diff") / col("open_s"))
    .select("ticker","date","price_diff","price_diff_perc")
    .distinct()
    .groupBy("ticker").agg(round(avg("price_diff_perc"),3).as("avg_perc_movement"))

// also filter out stocks with $0 latest price
// so join with our summary price table but get a price from the latest date
val latest_price_wind = Window.partitionBy("ticker").orderBy(col("date").desc)
val latest_price = summary
    .withColumn("row_rank", rank().over(latest_price_wind))
    .filter("row_rank == 1")
    .select("ticker", "close_s")

/* limit output to stocks with at least 1% average daily movement in price
 and mean_stdev > 0 ...these are unrealistic values likely due to stocks with extremely low
 trading volume
join predictability with volatility
*/ 
val results = predictability
    .join(volatility, Seq("ticker"))
    .filter("avg_perc_movement >= 0.01 and mean_stdev > 0 and mean_stdev <= 0.05")
    .join(latest_price, Seq("ticker"))

/* the results are a list of stocks that are candidates for a day-trading strategy
 these values indicate a stock that has daily pricemovements that do not vary much from day to day
 */

results
    .orderBy(col("avg_perc_movement").desc)
    .show()


