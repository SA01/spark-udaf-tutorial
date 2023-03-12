package com

import org.apache.spark.sql.SparkSession

package object tutorial {
  def createSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.caseSensitive", value = true)
      .config("spark.sql.session.timeZone", value="UTC")
      .appName(appName)
      .master("local[1]")
      .getOrCreate()
  }
}
