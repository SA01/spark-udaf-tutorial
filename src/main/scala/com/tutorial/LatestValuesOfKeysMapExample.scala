package com.tutorial

import org.apache.spark.sql.functions.{col, expr, to_timestamp, udaf}

object LatestValuesOfKeysMapExample {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession("AverageDaysSinceEarliestExample")
    import spark.implicits._

    spark.udf.register("LatestValuesOfKeysMapUdaf", udaf(LatestValuesOfKeysMapUdaf))

    val data = Seq(
      ("2022-03-11 09:00:00", "location_1", Map[String, Double]("temperature" -> 23.1, "air_quality" -> 12)),
      ("2022-03-11 09:01:00", "location_1", Map[String, Double]("temperature" -> 23.5, "air_quality" -> 10, "humidity" -> 68)),
      ("2022-03-11 09:02:00", "location_1", Map[String, Double]("temperature" -> 23.8, "air_quality" -> 13)),
      ("2022-03-11 09:03:00", "location_1", Map[String, Double]("temperature" -> 23.2, "air_quality" -> 11, "humidity" -> 65, "light_intensity" -> 1200)),
      ("2022-03-11 09:04:00", "location_1", Map[String, Double]("temperature" -> 23.4, "air_quality" -> 9)),
      ("2022-03-11 09:05:00", "location_1", Map[String, Double]("temperature" -> 23.9, "air_quality" -> 14, "humidity" -> 72)),
      ("2022-03-11 09:00:00", "location_2", Map[String, Double]("temperature" -> 19.8, "air_quality" -> 15)),
      ("2022-03-11 09:01:00", "location_2", Map[String, Double]("temperature" -> 20.1, "air_quality" -> 17, "humidity" -> 63)),
      ("2022-03-11 09:02:00", "location_2", Map[String, Double]("temperature" -> 20.3, "air_quality" -> 18)),
      ("2022-03-11 09:03:00", "location_2", Map[String, Double]("temperature" -> 19.7, "air_quality" -> 16, "humidity" -> 75, "light_intensity" -> 900)),
      ("2022-03-11 09:04:00", "location_2", Map[String, Double]("temperature" -> 20.0, "air_quality" -> 14)),
      ("2022-03-11 09:05:00", "location_2", Map[String, Double]("temperature" -> 20.4, "air_quality" -> 19, "humidity" -> 60))
    )

    // Convert the data to a DataFrame
    val testDf = data
      .toDF("timestamp", "location", "readings")
      .withColumn("timestamp", to_timestamp(col("timestamp")))
      .repartition(2, col("location"))
    testDf.show(truncate = false)
    testDf.printSchema()

    val resDf = testDf.groupBy("location").agg(expr("LatestValuesOfKeysMapUdaf(timestamp, readings)"))

    resDf.show(truncate = false)
  }
}
