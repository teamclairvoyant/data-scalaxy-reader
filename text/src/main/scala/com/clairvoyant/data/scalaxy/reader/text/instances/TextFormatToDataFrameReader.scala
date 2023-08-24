package com.clairvoyant.data.scalaxy.reader.text.instances

import org.apache.spark.sql.{DataFrame, SparkSession}

trait TextFormatToDataFrameReader[T]:

  def read(
      text: Seq[String],
      textFormat: T
  )(using sparkSession: SparkSession): DataFrame
