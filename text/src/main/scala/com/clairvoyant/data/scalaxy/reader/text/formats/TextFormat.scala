package com.clairvoyant.data.scalaxy.reader.text.formats

import org.apache.spark.sql.catalyst.util.{FailFastMode, ParseMode}
import org.apache.spark.sql.types.StructType
import zio.config.derivation.*

@nameWithLabel
trait TextFormat

case class CSVTextFormat(
    adaptSchemaColumns: StructType => StructType = identity,
    charToEscapeQuoteEscaping: String = "\\",
    columnNameOfCorruptRecord: String = "_corrupt_record",
    comment: String = "#",
    dateFormat: String = "yyyy-MM-dd",
    emptyValue: String = "",
    enableDateTimeParsingFallback: Boolean = true,
    encoding: String = "UTF-8",
    enforceSchema: Boolean = false,
    escape: String = "\\",
    header: Boolean = true,
    inferSchema: Boolean = true,
    ignoreLeadingWhiteSpace: Boolean = false,
    ignoreTrailingWhiteSpace: Boolean = false,
    lineSep: String = "\n",
    locale: String = "en-US",
    maxCharsPerColumn: Long = -1,
    maxColumns: Long = 20480,
    mode: ParseMode = FailFastMode,
    multiLine: Boolean = false,
    nanValue: String = "NaN",
    negativeInf: String = "-Inf",
    nullValue: String = "null",
    originalSchema: Option[StructType] = None,
    positiveInf: String = "Inf",
    preferDate: Boolean = true,
    quote: String = "\"",
    recordSep: String = "\n",
    samplingRatio: Double = 1.0,
    sep: String = ",",
    timestampFormat: String = "yyyy-MM-dd HH:mm:ss",
    timestampNTZFormat: String = "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
    unescapedQuoteHandling: String = "STOP_AT_DELIMITER"
) extends TextFormat

case class JSONTextFormat(
    adaptSchemaColumns: StructType => StructType = identity,
    allowBackslashEscapingAnyCharacter: Boolean = false,
    allowComments: Boolean = false,
    allowNonNumericNumbers: Boolean = true,
    allowNumericLeadingZeros: Boolean = false,
    allowSingleQuotes: Boolean = true,
    allowUnquotedControlChars: Boolean = false,
    allowUnquotedFieldNames: Boolean = false,
    columnNameOfCorruptRecord: String = "_corrupt_record",
    dataColumnName: Option[String] = None,
    dateFormat: String = "yyyy-MM-dd",
    dropFieldIfAllNull: Boolean = false,
    enableDateTimeParsingFallback: Boolean = true,
    encoding: String = "UTF-8",
    lineSep: String = "\n",
    locale: String = "en-US",
    mode: ParseMode = FailFastMode,
    multiLine: Boolean = false,
    originalSchema: Option[StructType] = None,
    prefersDecimal: Boolean = false,
    primitivesAsString: Boolean = false,
    samplingRatio: Double = 1.0,
    timestampFormat: String = "yyyy-MM-dd HH:mm:ss",
    timestampNTZFormat: String = "yyyy-MM-dd'T'HH:mm:ss[.SSS]",
    timeZone: String = "UTC"
) extends TextFormat

case class XMLTextFormat(
    adaptSchemaColumns: StructType => StructType = identity,
    attributePrefix: String = "_",
    charset: String = "UTF-8",
    columnNameOfCorruptRecord: String = "_corrupt_record",
    dateFormat: String = "yyyy-MM-dd",
    excludeAttribute: Boolean = false,
    ignoreSurroundingSpaces: Boolean = false,
    ignoreNamespace: Boolean = false,
    inferSchema: Boolean = true,
    mode: ParseMode = FailFastMode,
    nullValue: String = "null",
    originalSchema: Option[StructType] = None,
    rowTag: String = "row",
    samplingRatio: Double = 1.0,
    timestampFormat: String = "yyyy-MM-dd HH:mm:ss",
    valueTag: String = "_VALUE",
    wildcardColName: String = "xs_any"
) extends TextFormat
