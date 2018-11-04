/*
For each month, provide the hour and city that is best time to run
Best time and place defined as:

1) temp is closest to 50
2) Avg all temps within same city and same hour
3) tiebreaker is the least windy hour
4) further ties are reported together
*/
import org.apache.spark.sql.functions._
import spark.implicits._


// read in both cities and create a unix timestamp, select only data we need
val osh = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/Oshkosh/")
    .filter("TemperatureF > -9999")
    .select("Year","Month","Day", "TemperatureF", "TimeCST", "Wind SpeedMPH")
    .withColumn("hour", hour(to_timestamp(col("TimeCST"), "hh:mm a")))
    .withColumn("City", lit("Oshkosh"))

val iowa = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/IowaCity/")
    .filter("TemperatureF > -9999")
    .select("Year","Month","Day", "TemperatureF", "TimeCST", "Wind SpeedMPH")
    .withColumn("hour", hour(to_timestamp(col("TimeCST"), "hh:mm a")))
    .withColumn("City", lit("IowaCity"))

val both_cities = 
    iowa.union(osh)

val avgTemp = Window.partitionBy(col("City"), col("Month"), col("Hour"))
val tempRank = Window.partitionBy(col("Month")).orderBy(col("diff"))
val bestTemp = 50
val windRank = Window.partitionBy(col("Month")).orderBy(col("avg_wind"))
both_cities
    .withColumn("clean_wind", when(col("Wind SpeedMPH")===lit("Calm"),0).otherwise(col("Wind SpeedMPH")))//clean up wind
    .filter("clean_wind >= 0") // exclude the -9999 vals
    .withColumn("avg_temp", round(avg(col("TemperatureF")).over(avgTemp),8)) //calc avg temp per hour and month and city
    .withColumn("avg_wind", round(avg(col("Wind SpeedMPH")).over(avgTemp),8)) //calc avg wind speed per hour month city
    .withColumn("diff", abs(col("avg_temp")-lit(bestTemp))) // get diff of 50 and the avg temp
    .withColumn("temp_rank", rank().over(tempRank)) //get the rank, to filter on
    .filter("temp_rank == 1")
    .select("Month","City","hour","avg_temp","avg_wind")
    .withColumn("wind_rank", rank().over(windRank)) // implement the tie breaker
    .filter("wind_rank == 1") //but show both if there are ties in wind
    .distinct()
    .orderBy(col("Month"))
    .select("Month","City","Hour") // only display month, city and hour as results
    .show()

