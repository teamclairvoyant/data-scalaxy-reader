package com.clairvoyant.data.scalaxy.reader.text

import com.clairvoyant.data.scalaxy.reader.text.formats.TextFormat
import com.clairvoyant.data.scalaxy.reader.text.instances.TextFormatToDataFrameReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object TextToDataFrameReader {

  def read[T <: TextFormat](
      text: String,
      textFormat: T
  )(using textFormatToDataFrameReader: TextFormatToDataFrameReader[T], sparkSession: SparkSession): DataFrame =
    read(Seq(text), textFormat)

  def read[T <: TextFormat](
      text: Seq[String],
      textFormat: T
  )(using textFormatToDataFrameReader: TextFormatToDataFrameReader[T], sparkSession: SparkSession): DataFrame =
    textFormatToDataFrameReader.read(text, textFormat)

}
