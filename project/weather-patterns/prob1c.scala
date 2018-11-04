/*
For Oshkosh, what 7 day period was the hottest? 
By hottest I mean, the average temperature of all readings from 12:00am on day 1 to 11:59pm on day 7.
*/
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val osh = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/Oshkosh/")
    .filter("TemperatureF > -9999")

val unix_7Day = 86400*7

//window to lookback 7 days in unix time
val w1 = Window.orderBy(col("unix_time").cast("long").desc).rangeBetween(-unix_7Day, 0)
//window to calculate rank of each 7day window
val w2 = Window.orderBy(col("7_day_avg").desc)

// create a unix date for rolling window
// then compute 7 day average using window1 (look back 7 unix days)
// compute the rank of each average, descending (we want hottest value = 1)
// then show us the hottest Year, Month and Day
osh
    .select("Year","Month","Day","TemperatureF")
    .withColumn("dec_date", concat($"Year", format_string("%02d", $"Month"),format_string("%02d", $"Day")))
    .withColumn("unix_time", unix_timestamp(col("dec_date"), "yyyyMMdd"))
    .withColumn("7_day_avg", round(avg($"TemperatureF").over(w1),6))
    .withColumn("rank", rank().over(w2))
    .filter("rank==1")
    .select("Year","Month","Day","7_day_avg")
    .distinct()
    .show()