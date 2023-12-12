package com.clairvoyant.data.scalaxy.reader.text.instances

import com.clairvoyant.data.scalaxy.reader.text.TextToDataFrameReader
import com.clairvoyant.data.scalaxy.reader.text.formats.XMLTextFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{File, PrintWriter}
import java.util.concurrent.Semaphore

implicit object XMLTextToDataFrameReader extends TextToDataFrameReader[XMLTextFormat] {

  override def read(
      text: Seq[String],
      textFormat: XMLTextFormat,
      originalSchema: Option[StructType],
      adaptSchemaColumns: StructType => StructType
  )(using sparkSession: SparkSession): DataFrame =

    import sparkSession.implicits.*

    def saveXMLTextToTempFiles(xmlTextValue: Seq[String]) = {
      xmlTextValue.map { text =>
        val file = File.createTempFile("xml-text-", ".xml")
        file.deleteOnExit()
        new PrintWriter(file) {
          try {
            write(text)
          } finally {
            close()
          }
        }
        file
      }
    }

    /*
        This is needed because SparkSession is not thread safe.
        If we try to read different files with the same session,
        Spark XML reader chooses random files to read
     */
    val semaphore = new Semaphore(1)
    semaphore.acquire()

    val tempXMLFiles = saveXMLTextToTempFiles(text)
    val tempXMLFilesPaths = tempXMLFiles.map(_.getAbsolutePath).mkString(",")

    val xmlDataFrameReader = sparkSession.read
      .options(
        Map(
          "attributePrefix" -> textFormat.attributePrefix,
          "charset" -> textFormat.charset,
          "columnNameOfCorruptRecord" -> textFormat.columnNameOfCorruptRecord,
          "dateFormat" -> textFormat.dateFormat,
          "excludeAttribute" -> textFormat.excludeAttribute,
          "ignoreSurroundingSpaces" -> textFormat.ignoreSurroundingSpaces,
          "ignoreNamespace" -> textFormat.ignoreNamespace,
          "inferSchema" -> textFormat.inferSchema,
          "mode" -> textFormat.mode,
          "nullValue" -> textFormat.nullValue,
          "rowTag" -> textFormat.rowTag,
          "samplingRatio" -> textFormat.samplingRatio,
          "timestampFormat" -> textFormat.timestampFormat,
          "valueTag" -> textFormat.valueTag,
          "wildcardColName" -> textFormat.wildcardColName
        ).map((optionName, optionValue) => (optionName, optionValue.toString()))
      )
      .format("xml")

    val xmlDataFrame = xmlDataFrameReader
      .schema {
        originalSchema.getOrElse {
          adaptSchemaColumns {
            xmlDataFrameReader
              .load(tempXMLFilesPaths)
              .schema
          }
        }
      }
      .load(tempXMLFilesPaths)

    semaphore.release()

    xmlDataFrame

}
