/*
Compute the average temperature 
(sum all temperatures in the time period and divide by the number of readings) 
for each season for Oshkosh and Iowa City. 
What is the difference in average temperatures for each season for Oshkosh vs Iowa City?
*/

import org.apache.spark.sql.functions._

val osh = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/Oshkosh/")
    .filter("TemperatureF > -9999")

val iowa = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/IowaCity/")
    .filter("TemperatureF > -9999")

// define seasons
val season = udf((month : Integer) => {
    if (month==12 | (month >=1 & month<3)) "Winter"
    else if (month >=3 & month < 6) "Spring"
    else if (month >=6 & month < 9) "Summer"
    else if (month >=9 & month < 12) "Fall"
    else "Err"
})

// compute mean TempF by season
val iowa_season = iowa
    .select("Month","TemperatureF")
    .withColumn("season", season(col("Month")))
    .groupBy("season").agg(avg(col("TemperatureF")).as("iowa_Avg"))

val osh_season = osh
    .select("Month","TemperatureF")
    .withColumn("season", season(col("Month")))
    .groupBy("season").agg(avg(col("TemperatureF")).as("osh_avg"))

// join two cities and subtract
iowa_season.join(osh_season, "season")
    .withColumn("Oshkosh-Iowa", round(col("osh_avg") - col("iowa_avg"),3))
    .select("season","Oshkosh-Iowa")
    .show()
    
    