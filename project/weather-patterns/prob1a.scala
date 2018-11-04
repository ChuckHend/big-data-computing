/*
In Oshkosh, which is more common: days where the temperature was really cold 
(-10 or lower) or days where the temperature was hot (95 or higher)?
*/
import org.apache.spark.sql.functions._

val osh = spark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/user/maria_dev/final/Oshkosh/")

//assume that there wont be a day with temps both above 95 and below -10
val temp = udf((high: Float, low: Float) => {
  if (high >= 95 ) "Hot"
  else if (low <= -10) "Cold"
  else "Neutral"
})

osh
  .select("Year","Month","Day","TemperatureF") //select only what we need
  .filter("TemperatureF > -9999") //filter out the erroneous values
  .groupBy($"Year",$"Month",$"Day") //each row is a single day
  .agg(temp(max($"TemperatureF"), min($"TemperatureF")).as("status"))//apply the udf
  .groupBy("status") //reduce to three rows
  .count() //count the values
  .show()