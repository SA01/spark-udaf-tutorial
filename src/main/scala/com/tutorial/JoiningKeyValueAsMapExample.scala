package com.tutorial

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, functions}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{col, expr}

import scala.collection.mutable.{Map => MutableMap}


case class KeyValue(key: String, value: String)
object MergeKeyValuesUdaf extends Aggregator[KeyValue, MutableMap[String, String], Map[String, String]] {
  override def zero: MutableMap[String, String] = MutableMap[String, String]()

  override def reduce(intValue: MutableMap[String, String], newValue: KeyValue): MutableMap[String, String] = {
    println(s"New value: ${newValue}")
    intValue + (newValue.key -> newValue.value)
  }

  override def merge(b1: MutableMap[String, String], b2: MutableMap[String, String]): MutableMap[String, String] = {
    val res = b1.++=(b2)
    println(s"Merge result: ${res} - b1: ${b1} - b2: ${b2}")
    res
  }

  override def finish(reduction: MutableMap[String, String]): Map[String, String] = {
    println(s"finish: ${reduction}")
    reduction.toMap
  }

  override def bufferEncoder: Encoder[MutableMap[String, String]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Map[String, String]] = ExpressionEncoder()
}

object JoiningKeyValueAsMapExample {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession("AverageDaysSinceEarliestExample")
    import spark.implicits._

    spark.udf.register("MergeKeyValuesUdaf", functions.udaf(MergeKeyValuesUdaf))

    val testDf = Seq(
      ("grp1", "Key1", "Value1"),
      ("grp1", "Key2", "Value2"),
      ("grp1", "Key3", "Value3"),
      ("grp1", "Key4", "Value4"),
      ("grp1", "Key5", "Value5"),
      ("grp2", "Key6", "Value6"),
      ("grp2", "Key7", "Value7"),
      ("grp2", "Key8", "Value8"),
      ("grp2", "Key9", "Value9"),
      ("grp2", "Key10", "Value10"),
    )
      .toDF("group", "keys", "values")
      .repartition(2, col("group"))

    val resDf = testDf
      .groupBy("group").agg(expr("MergeKeyValuesUdaf(keys, values)"))

    resDf.show(truncate = false)
    resDf.printSchema()

    /*
    * Link about ExpressionEncoder: https://books.japila.pl/spark-sql-internals/ExpressionEncoder/
    * */

  }
}
