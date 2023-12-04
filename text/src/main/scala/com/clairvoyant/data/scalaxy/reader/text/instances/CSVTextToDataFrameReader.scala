package com.clairvoyant.data.scalaxy.reader.text.instances

import com.clairvoyant.data.scalaxy.reader.text.TextToDataFrameReader
import com.clairvoyant.data.scalaxy.reader.text.formats.CSVTextFormat
import org.apache.spark.sql.catalyst.csv.CSVOptions.*
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

implicit object CSVTextToDataFrameReader extends TextToDataFrameReader[CSVTextFormat] {

  override def read(
      text: Seq[String],
      textFormat: CSVTextFormat,
      originalSchema: Option[StructType],
      adaptSchemaColumns: StructType => StructType
  )(using sparkSession: SparkSession): DataFrame =

    import sparkSession.implicits.*

    val csvDataset = text
      .flatMap(_.split(textFormat.recordSep))
      .toDS()

    val csvDataFrameReader = sparkSession.read
      .option(CHAR_TO_ESCAPE_QUOTE_ESCAPING, textFormat.charToEscapeQuoteEscaping)
      .option(COLUMN_NAME_OF_CORRUPT_RECORD, textFormat.columnNameOfCorruptRecord)
      .option(COMMENT, textFormat.comment)
      .option(DATE_FORMAT, textFormat.dateFormat)
      .option(EMPTY_VALUE, textFormat.emptyValue)
      .option(ENABLE_DATETIME_PARSING_FALLBACK, textFormat.enableDateTimeParsingFallback)
      .option(ENCODING, textFormat.encoding)
      .option(ENFORCE_SCHEMA, textFormat.enforceSchema)
      .option(ESCAPE, textFormat.escape)
      .option(HEADER, textFormat.header)
      .option(INFER_SCHEMA, textFormat.inferSchema)
      .option(IGNORE_LEADING_WHITESPACE, textFormat.ignoreLeadingWhiteSpace)
      .option(IGNORE_TRAILING_WHITESPACE, textFormat.ignoreTrailingWhiteSpace)
      .option(LINE_SEP, textFormat.lineSep)
      .option(LOCALE, textFormat.locale)
      .option(MAX_CHARS_PER_COLUMN, textFormat.maxCharsPerColumn)
      .option(MAX_COLUMNS, textFormat.maxColumns)
      .option(MODE, textFormat.mode)
      .option(MULTI_LINE, textFormat.multiLine)
      .option(NAN_VALUE, textFormat.nanValue)
      .option(NEGATIVE_INF, textFormat.negativeInf)
      .option(NULL_VALUE, textFormat.nullValue)
      .option(POSITIVE_INF, textFormat.positiveInf)
      .option(PREFER_DATE, textFormat.preferDate)
      .option(QUOTE, textFormat.quote)
      .option(SAMPLING_RATIO, textFormat.samplingRatio)
      .option(SEP, textFormat.sep)
      .option(TIMESTAMP_FORMAT, textFormat.timestampFormat)
      .option(TIMESTAMP_NTZ_FORMAT, textFormat.timestampNTZFormat)
      .option(UNESCAPED_QUOTE_HANDLING, textFormat.unescapedQuoteHandling)

    csvDataFrameReader
      .schema {
        originalSchema
          .getOrElse {
            adaptSchemaColumns {
              if (textFormat.mode == "PERMISSIVE") {
                csvDataFrameReader
                  .csv(csvDataset)
                  .schema
                  .add(name = textFormat.columnNameOfCorruptRecord, dataType = StringType, nullable = true)
              } else {
                csvDataFrameReader
                  .csv(csvDataset)
                  .schema
              }
            }
          }
      }
      .csv(csvDataset)

}
