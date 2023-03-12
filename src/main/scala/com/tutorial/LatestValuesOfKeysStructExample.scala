package com.tutorial

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, expr, struct, to_timestamp, udaf}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}

object LatestValuesOfKeysStructExample {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession("AverageDaysSinceEarliestExample")
    import spark.implicits._

    val inputSchema = StructType(
      List(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("readings", StructType(
          List(
            StructField("temperature", DoubleType, nullable = false),
            StructField("air_quality", IntegerType, nullable = false),
            StructField("humidity", IntegerType, nullable = true),
            StructField("light_intensity", IntegerType, nullable = true)
          )
        ), nullable = false)
      )
    )
    spark.udf.register(
      "LatestValuesOfFieldsAggregation",
      udaf(LatestValuesOfFieldsUdaf, inputEncoder = RowEncoder(inputSchema))
    )

    val data = Seq(
      ("2022-03-11 09:00:00", "location_1", 23.1, 12, None, None),
      ("2022-03-11 09:01:00", "location_1", 23.5, 10, Some(68), None),
      ("2022-03-11 09:02:00", "location_1", 23.8, 13, None, None),
      ("2022-03-11 09:03:00", "location_1", 23.2, 11, Some(65), Some(1200)),
      ("2022-03-11 09:04:00", "location_1", 23.4, 9, None, None),
      ("2022-03-11 09:05:00", "location_1", 23.9, 14, Some(72), None),
      ("2022-03-11 09:00:00", "location_2", 19.8, 15, None, None),
      ("2022-03-11 09:01:00", "location_2", 20.1, 17, Some(63), None),
      ("2022-03-11 09:02:00", "location_2", 20.3, 18, None, None),
      ("2022-03-11 09:03:00", "location_2", 19.7, 16, Some(75), Some(900)),
      ("2022-03-11 09:04:00", "location_2", 20.0, 14, None, None),
      ("2022-03-11 09:05:00", "location_2", 20.4, 19, Some(60), None)
    )

    // Convert the data to a DataFrame
    val testDf = data
      .toDF("timestamp", "location", "temperature", "air_quality", "humidity", "light_intensity")
      .withColumn(
        "readings",
        struct("temperature", "air_quality", "humidity", "light_intensity")
      )
      .drop("temperature", "air_quality", "humidity", "light_intensity")
      .withColumn("timestamp", to_timestamp(col("timestamp")))
      .repartition(2, col("location"))

    testDf.show(truncate = false)
    testDf.printSchema()

    val resDf = testDf.groupBy("location").agg(expr("LatestValuesOfFieldsAggregation(timestamp, readings)"))

    resDf.show(truncate = false)
  }
}
