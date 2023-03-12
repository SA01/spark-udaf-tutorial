package com.tutorial

import org.apache.spark.sql.{Encoder, SparkSession, functions}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable.{Map => MutableMap}
import java.sql.Timestamp

case class DataColumns(timestamp: Timestamp, readings: Map[String, Double])
case class ValueWithTime(var timestamp: Timestamp, var value: Double)
object LatestValuesOfKeysMapUdaf extends Aggregator[DataColumns, MutableMap[String, ValueWithTime], Map[String, Double]] {
  override def zero: MutableMap[String, ValueWithTime] = MutableMap[String, ValueWithTime]()

  override def reduce(buffer: MutableMap[String, ValueWithTime], newValue: DataColumns): MutableMap[String, ValueWithTime] = {
    val newValueTime = newValue.timestamp
    val newValueReadings = newValue.readings

    println(s"*" * 100)
    println(s"** Reduce call before \nnewValue: ${newValue} \nbuffer: ${buffer}")

    val resValue = newValueReadings.foldLeft(buffer)((runningBuffer, newReading) => {
      if (!runningBuffer.contains(newReading._1)) {
        runningBuffer += (newReading._1 -> ValueWithTime(timestamp = newValueTime, value = newReading._2))
        runningBuffer
      }
      else if (runningBuffer.contains(newReading._1) && runningBuffer(newReading._1).timestamp.before(newValueTime)) {
        runningBuffer(newReading._1).timestamp = newValueTime
        runningBuffer(newReading._1).value = newReading._2
        runningBuffer
      } else {
        runningBuffer
      }
    })

    println(s"** Reduce call after \nresValue: ${resValue}")
    println(s"*" * 100)
    resValue
  }

  override def merge(b1: MutableMap[String, ValueWithTime], b2: MutableMap[String, ValueWithTime]): MutableMap[String, ValueWithTime] = {
    println("*" * 100)
    println(s"merge call:\nb1: ${b1}\nb2: ${b2}")

    b2.foreach(b2Item => {
      if (!b1.contains(b2Item._1)) {
        b1 += (b2Item._1 -> ValueWithTime(b2Item._2.timestamp, b2Item._2.value))
      }
      if (b1.contains(b2Item._1) && b1(b2Item._1).timestamp.before(b2Item._2.timestamp)) {
        b1(b2Item._1).timestamp = b2Item._2.timestamp
        b1(b2Item._1).value = b2Item._2.value
        b1
      } else {
        b1
      }
    })

    println(s"merge after:\nb1: ${b1}")
    println("*" * 100)
    b1
  }

  override def finish(reduction: MutableMap[String, ValueWithTime]): Map[String, Double] = {
    reduction.map(redItem => redItem._1 -> redItem._2.value).toMap
  }

  override def bufferEncoder: Encoder[MutableMap[String, ValueWithTime]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Map[String, Double]] = ExpressionEncoder()

  def register(spark: SparkSession): Unit = {
    spark.udf.register("LatestValuesOfKeysMapUdaf", functions.udaf(LatestValuesOfKeysMapUdaf))
  }
}
