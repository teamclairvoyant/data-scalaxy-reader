package com.clairvoyant.data.scalaxy.reader.text

import com.clairvoyant.data.scalaxy.reader.text.formats.TextFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait TextToDataFrameReader[T]:

  def read(
      text: Seq[String],
      textFormat: T,
      originalSchema: Option[StructType],
      adaptSchemaColumns: StructType => StructType
  )(using sparkSession: SparkSession): DataFrame

  def read(
      text: String,
      textFormat: T,
      originalSchema: Option[StructType] = None,
      adaptSchemaColumns: StructType => StructType = identity
  )(using sparkSession: SparkSession): DataFrame = read(Seq(text), textFormat, originalSchema, adaptSchemaColumns)

object TextToDataFrameReader:

  def apply[T <: TextFormat](using textToDataFrameReader: TextToDataFrameReader[T]): TextToDataFrameReader[T] =
    textToDataFrameReader
