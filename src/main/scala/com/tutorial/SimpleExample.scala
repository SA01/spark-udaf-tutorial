package com.tutorial

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions.{col, concat, expr, lit}

object SumUdaf extends Aggregator[Int, Int, Int] {
  override def zero: Int = 0

  override def reduce(buffer: Int, newValue: Int): Int = {
    println(s"Reduce called: buffer: ${buffer} - newValue: ${newValue}")
    buffer + newValue
  }

  override def merge(intermediateValue1: Int, intermediateValue2: Int): Int = {
    println(s"Merge called: ival1: ${intermediateValue1} - ival2: ${intermediateValue2}")
    intermediateValue1 + intermediateValue2
  }

  override def finish(reduction: Int): Int = {
    println(s"Finish called: ${reduction}")
    reduction
  }

  override def bufferEncoder: Encoder[Int] = Encoders.scalaInt

  override def outputEncoder: Encoder[Int] = Encoders.scalaInt
}

case class IntermediateAverageValues(var sum: Int, var count: Int)
object AverageUdaf extends Aggregator[Int, IntermediateAverageValues, Float] {
  override def zero: IntermediateAverageValues = {
    println("Zero called")
    IntermediateAverageValues(0, 0)
  }

  override def reduce(buffer: IntermediateAverageValues, newValue: Int): IntermediateAverageValues = {
    println(s"Reduce called: buffer: ${buffer} - newValue: ${newValue}")
    buffer.sum += newValue
    buffer.count += 1
    buffer
  }

  override def merge(intermediateValue1: IntermediateAverageValues, intermediateValue2: IntermediateAverageValues): IntermediateAverageValues = {
    println(s"Merge called: ival1: ${intermediateValue1} - ival2: ${intermediateValue2}")
    intermediateValue1.sum += intermediateValue2.sum
    intermediateValue1.count += intermediateValue2.count
    intermediateValue1
  }

  override def finish(reduction: IntermediateAverageValues): Float = {
    println(s"Finish called: ${reduction}")
    (reduction.sum * 1.0f) / reduction.count
  }

  override def bufferEncoder: Encoder[IntermediateAverageValues] = Encoders.product

  override def outputEncoder: Encoder[Float] = Encoders.scalaFloat
}

object SimpleExample {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = createSparkSession("UdafTutorial")

    // Register the UDAFs
    spark.udf.register("SumUdaf", functions.udaf(SumUdaf))
    spark.udf.register("AverageUdaf", functions.udaf(AverageUdaf))

    import spark.implicits._

    // Create a test DataFrame
    val testDf = Seq(
      (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
    )
      .toDF("number")
      .withColumn("mod", col("number") % 2)

    // Display the contents of the DataFrame in the console
    testDf.show(truncate = false)

    // UDAF usage
    testDf
      .groupBy("mod")
      .agg(
        expr("AverageUdaf(number)")
      )
      .show(truncate = false)

    // UDAF usage as window function
    val window = Window.partitionBy("mod")
    testDf
      .withColumn("group_avg", expr("AverageUdaf(number)").over(window))
      .show(truncate = false)

    // Uncomment the following code block to see how the UDAF behaves when the input values are Strings rather than Integers
    //    testDf
    //      .withColumn("number", col("number").cast(StringType))
    //      .groupBy("mod")
    //      .agg(expr("SumUdaf(number)"))
    //      .show(truncate = false)
  }
}

