package com.clairvoyant.data.scalaxy.reader.text.instances

import com.clairvoyant.data.scalaxy.reader.text.TextToDataFrameReader
import com.clairvoyant.data.scalaxy.reader.text.formats.{CSVTextFormat, HTMLTableTextFormat}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.Try

implicit object HTMLTableTextToDataFrameReader extends TextToDataFrameReader[HTMLTableTextFormat] {

  private val VALUE_SEPARATOR = "~"

  override def read(
      text: Seq[String],
      textFormat: HTMLTableTextFormat,
      originalSchema: Option[StructType],
      adaptSchemaColumns: StructType => StructType
  )(using sparkSession: SparkSession): DataFrame =
    TextToDataFrameReader[CSVTextFormat].read(
      text = text.map(htmlText => convertHTMLTableToCSV(htmlText, textFormat.tableName)),
      textFormat = CSVTextFormat(
        sep = VALUE_SEPARATOR
      ),
      originalSchema = originalSchema,
      adaptSchemaColumns = adaptSchemaColumns
    )

  private def convertHTMLTableToCSV(htmlText: String, tableName: Option[String] = None): String = {
    Try {
      val parsedDocument = Jsoup.parse(htmlText)

      val table = getTableFromParsedDocument(parsedDocument, tableName)

      val rows = table.select("tr")
      val tableHeader = getTableHeader(rows)
      val tableRows = getTableRows(rows)

      tableHeader.concat(tableRows)
    }.recover { case ex: Exception =>
      ex.printStackTrace()
      throw ex
    }.get
  }

  private def getTableFromParsedDocument(parsedDocument: Document, tableName: Option[String]): Element =
    tableName
      .map { tblName =>
        val tables = parsedDocument.select(tblName)
        if (tables.size() > 0)
          tables.first()
        else
          throw new Exception(s"HTML table: $tblName not found")
      }
      .getOrElse(parsedDocument.getElementsByTag("table").first())

  private def getTableHeader(rows: Elements): String =
    Seq(rows.select("th").map(_.text)).flatten
      .mkString(VALUE_SEPARATOR)
      .concat("\n")

  private def getTableRows(rows: Elements): String =
    Seq(
      rows.map { row =>
        val flattenRows = Seq(row.select("td").map(_.text())).flatten
        if (flattenRows.nonEmpty)
          flattenRows.mkString(VALUE_SEPARATOR).concat("\n")
        else
          ""
      }
    ).flatten.mkString("")

}
