package com.clairvoyant.data.scalaxy.reader.text.instances

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait TextFormatToDataFrameReader[T]:

  def read(
      text: Seq[String],
      textFormat: T,
      originalSchema: Option[StructType],
      adaptSchemaColumns: StructType => StructType
  )(using sparkSession: SparkSession): DataFrame
