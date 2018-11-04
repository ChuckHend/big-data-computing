/*
  by sector, what time of day shows the avg peak price for all the stocks in the industry? aggregate to the hour
  for every ticker, get the time of the days peak price
  by sector, aggregate the counts of peaks by hour
  result should be a distribution of peaks per hour of day, by sector
  visulize the distribution for each sector (13 of them)
  export to .csv, create freq distribution (see report)
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


val raw = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/stocks")

val industry = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/exchange")
    .select(col("Symbol").as("ticker"), col("Sector"), col("industry"))


val sector_count = Window.partitionBy("sector")

val daily_peak = Window.partitionBy("ticker","date").orderBy(col("close").desc)
raw
    .join(industry, Seq("ticker"))
    .withColumn("hour", format_string("%04d", col("time")))
    .withColumn("hr", $"hour".substr(lit(0), lit(2)))
    .withColumn("peak", rank.over(daily_peak))
    .filter("peak==1")
    .groupBy("sector","hr").agg(sum("peak").as("count"))
    .withColumn("count_scaled", col("count") / sum(col("count")).over(sector_count))
    .coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("/user/maria_dev/final/sector_peaks/sector-peaks.csv")