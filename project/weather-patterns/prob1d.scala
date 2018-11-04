/*
For each day, determine coldest time of that day
Coldest time for a day is the hour that has coldest average
So need to average by hour, then take hour where min temp is lowest in day

Return the hour that has the hour that has the most occurrences of the coldest average
for each day
*/
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val osh = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/Oshkosh/")
    .filter("TemperatureF > -9999")
    .select("Year","Month","Day","TimeCST","TemperatureF")

// for every day, determine the lowest average hourly temp
val low_daily =  Window.partitionBy("Year","Month","Day").orderBy(col("avg_hr_temp").asc)
// rank each count by occurences of num_lowest temperatures
val hr_count = Window.orderBy(col("num_lowest").desc)

osh
    .groupBy("Year","Month","Day","TimeCST").agg(avg(col("TemperatureF")).as("avg_hr_temp"))
    .withColumn("daily_rnk", rank().over(low_daily)) //get rank for each avg_hr_temp
    .filter("daily_rnk==1") // select only the coldest hours
    .groupBy("TimeCST").agg(sum("daily_rnk").as("num_lowest")) //sum the number of "1" ranks
    .withColumn("time_rnk", rank().over(hr_count)) // determine which hour has most occurences
    .filter("time_rnk==1") // select only the top, with ties
    .select("TimeCST")
    .show()

