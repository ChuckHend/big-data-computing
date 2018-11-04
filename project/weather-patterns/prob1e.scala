/*
which city had a time period of 24 hours or less that saw the largest temp difference?
this must be a rolling 24 hour period
so calc the max-min temp difference for each 24 hour period for both cities

and the amount of time that difference spanned

*/

//TODO: get date/time of the max and min temps

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


// read in both cities and create a unix timestamp, select only data we need
val osh = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/Oshkosh/")
    .filter("TemperatureF > -9999")
    .select("Year","Month","Day", "TemperatureF", "TimeCST")
    .withColumn("dec_date", concat($"Year", format_string("%02d", $"Month"),format_string("%02d", $"Day"), lit(" "),col("TimeCST")))
    .withColumn("unix_time", unix_timestamp(col("dec_date"), "yyyyMMdd hh:mm a"))

val iowa = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/IowaCity/")
    .filter("TemperatureF > -9999")
    .select("Year","Month","Day", "TemperatureF", "TimeCST")
    .withColumn("dec_date", concat($"Year", format_string("%02d", $"Month"),format_string("%02d", $"Day"), lit(" "),col("TimeCST")))
    .withColumn("unix_time", unix_timestamp(col("dec_date"), "yyyyMMdd hh:mm a"))

val unix_day = 86400

//itermediate results
val both_cities = 
osh
    .withColumn("one_day_diff", 
        max(col("TemperatureF")).over(one_day) - min(col("TemperatureF")).over(one_day))
    .withColumn("City", lit("Oshkosh"))
    .groupBy("City").agg(max(col("one_day_diff")).as("Max_Diff"))
    .union(
iowa
    .withColumn("one_day_diff", 
        max(col("TemperatureF")).over(one_day) - min(col("TemperatureF")).over(one_day))
    .withColumn("City", lit("IowaCity"))
    .groupBy("City").agg(max(col("one_day_diff")).as("Max_Diff"))
    )
// so IowaCity has the largest diff....get the date ranges, join to this tale
// store this for now, get the times and join later

//window to lookback 1 day in unix time
val one_day = Window.orderBy(col("unix_time").cast("long")).rangeBetween(-unix_day, 0)
// window to determine rank of the diff
val day_rank = Window.orderBy(col("one_day_diff").desc)

// calculate the rank of temp diffs for each window
val osh_df = osh
    .withColumn("24_min", min(col("TemperatureF")).over(one_day))
    .withColumn("24_max", max(col("TemperatureF")).over(one_day))
    .withColumn("one_day_diff",  max(col("TemperatureF")).over(one_day) - min(col("TemperatureF")).over(one_day))
    .withColumn("diff_rnk", rank.over(day_rank))
val iowa_df = iowa
    .withColumn("24_min", min(col("TemperatureF")).over(one_day))
    .withColumn("24_max", max(col("TemperatureF")).over(one_day))
    .withColumn("one_day_diff",  max(col("TemperatureF")).over(one_day) - min(col("TemperatureF")).over(one_day))
    .withColumn("diff_rnk", rank.over(day_rank))

// take a subset for each city
// these DFs are just the window that contains the max diff for each city
val osh_wind = osh_df
    .filter(col("unix_time").lt(osh_end_date))
    .filter(col("unix_time").gt(osh_start_date))
val iowa_wind = iowa_df
    .filter(col("unix_time").lt(iowa_end_date))
    .filter(col("unix_time").gt(iowa_start_date))

//get the dates for the windows we are concerned about for each city
val osh_end_date = osh_df.filter("diff_rnk==1").select(min(col("unix_time"))).first().getLong(0).toInt
val osh_start_date = osh_end_date-unix_day.toInt
val iowa_end_date = iowa_df.filter("diff_rnk==1").select(min(col("unix_time"))).first().getLong(0).toInt
val iowa_start_date = iowa_end_date-unix_day.toInt

val min_time_wind = Window.orderBy(col("24_min").asc)
val max_time_wind = Window.orderBy(col("24_max").desc)

// get min and max times of days for the max diff window  for both cities
val osh_min_time = osh_wind
    .withColumn("min_rk", rank().over(min_time_wind))
    .filter("min_rk==1")
    .select(min(col("dec_date")).as("Oshkosh_Start_Date"))
    .withColumn("City", lit("Oshkosh"))   
val osh_max_time = osh_wind
    .withColumn("max_rk", rank().over(max_time_wind))
    .filter("max_rk==1")
    .select(min(col("dec_date")).as("Oshkosh_End_Date"))
    .withColumn("City", lit("Oshkosh"))
val iowa_min_time = iowa_wind
    .withColumn("min_rk", rank().over(min_time_wind))
    .filter("min_rk==1")
    .select(min(col("dec_date")).as("Iowa_Start_Date"))
    .withColumn("City", lit("IowaCity"))
val iowa_max_time = iowa_wind
    .withColumn("max_rk", rank().over(max_time_wind))
    .filter("max_rk==1")
    .select(min(col("dec_date")).as("Iowa_End_Date"))
    .withColumn("City", lit("IowaCity"))

// do some intermediate unions and joins, then join back to the resultsfrom earlier
val mindf = osh_min_time.union(iowa_min_time)
val maxdf = osh_max_time.union(iowa_max_time)

val minMax = mindf.join(maxdf, Seq("City"))
// print results
minMax
    .join(both_cities, Seq("City"))
    .show()