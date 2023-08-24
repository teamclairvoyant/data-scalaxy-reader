package com.clairvoyant.data.scalaxy.reader.text.instances

import com.clairvoyant.data.scalaxy.reader.text.formats.JSONTextFormat
import org.apache.spark.sql.catalyst.json.JSONOptions.*
import org.apache.spark.sql.{DataFrame, SparkSession}

implicit object JSONTextToDataFrameReader extends TextFormatToDataFrameReader[JSONTextFormat] {

  override def read(
      text: Seq[String],
      textFormat: JSONTextFormat
  )(using sparkSession: SparkSession): DataFrame =

    import sparkSession.implicits.*

    val jsonDataset = text.toDS()

    val jsonDataFrameReader = sparkSession.read
      .option(ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, textFormat.allowBackslashEscapingAnyCharacter)
      .option(ALLOW_COMMENTS, textFormat.allowComments)
      .option(ALLOW_NON_NUMERIC_NUMBERS, textFormat.allowNonNumericNumbers)
      .option(ALLOW_NUMERIC_LEADING_ZEROS, textFormat.allowNumericLeadingZeros)
      .option(ALLOW_SINGLE_QUOTES, textFormat.allowSingleQuotes)
      .option(ALLOW_UNQUOTED_CONTROL_CHARS, textFormat.allowUnquotedControlChars)
      .option(ALLOW_UNQUOTED_FIELD_NAMES, textFormat.allowUnquotedFieldNames)
      .option(COLUMN_NAME_OF_CORRUPTED_RECORD, textFormat.columnNameOfCorruptRecord)
      .option(DATE_FORMAT, textFormat.dateFormat)
      .option(DROP_FIELD_IF_ALL_NULL, textFormat.dropFieldIfAllNull)
      .option(ENABLE_DATETIME_PARSING_FALLBACK, textFormat.enableDateTimeParsingFallback)
      .option(ENCODING, textFormat.encoding)
      .option(LINE_SEP, textFormat.lineSep)
      .option(LOCALE, textFormat.locale)
      .option(MODE, textFormat.mode.name)
      .option(MULTI_LINE, textFormat.multiLine)
      .option(PREFERS_DECIMAL, textFormat.prefersDecimal)
      .option(PRIMITIVES_AS_STRING, textFormat.primitivesAsString)
      .option(SAMPLING_RATIO, textFormat.samplingRatio)
      .option(TIMESTAMP_FORMAT, textFormat.timestampFormat)
      .option(TIMESTAMP_NTZ_FORMAT, textFormat.timestampNTZFormat)
      .option(TIME_ZONE, textFormat.timeZone)

    jsonDataFrameReader
      .schema {
        textFormat.originalSchema.getOrElse {
          textFormat.adaptSchemaColumns {
            jsonDataFrameReader
              .json(jsonDataset)
              .schema
          }
        }
      }
      .json(jsonDataset)

}
