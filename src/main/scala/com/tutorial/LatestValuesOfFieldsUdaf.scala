package com.tutorial

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import scala.collection.mutable.{Map => MutableMap}

import java.sql.Timestamp

object LatestValuesOfFieldsUdaf extends Aggregator[Row, MutableMap[String, ValueWithTime], Map[String, Double]] {
  override def zero: MutableMap[String, ValueWithTime] = MutableMap[String, ValueWithTime]()

  override def reduce(buffer: MutableMap[String, ValueWithTime], newValue: Row): MutableMap[String, ValueWithTime] = {
    val newValueTime = newValue.getAs[Timestamp](newValue.fieldIndex("timestamp"))
    val newValueReadings = newValue.getAs[Row](newValue.fieldIndex("readings"))

    println(s"*" * 100)
    println(s"** Reduce call before \nnewValue: ${newValue} \nbuffer: ${buffer}")

    val resValue = newValueReadings
      .getValuesMap[Any](List("temperature", "air_quality", "humidity", "light_intensity"))
      .foldLeft(buffer)((runningBuffer, newReading) => {
        if (!runningBuffer.contains(newReading._1) && newReading._2 != null) {
          runningBuffer += (newReading._1 -> ValueWithTime(timestamp = newValueTime, value = newReading._2.asInstanceOf[Number].doubleValue()))
          runningBuffer
        }
        else if (runningBuffer.contains(newReading._1) && runningBuffer(newReading._1).timestamp.before(newValueTime) && newReading._2 != null) {
          runningBuffer(newReading._1).timestamp = newValueTime
          runningBuffer(newReading._1).value = newReading._2.asInstanceOf[Number].doubleValue()
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
}
