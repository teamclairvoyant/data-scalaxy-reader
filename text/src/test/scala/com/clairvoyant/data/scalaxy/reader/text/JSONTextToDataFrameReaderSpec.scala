package com.clairvoyant.data.scalaxy.reader.text

import com.clairvoyant.data.scalaxy.reader.text.formats.JSONTextFormat
import com.clairvoyant.data.scalaxy.reader.text.instances.JSONTextToDataFrameReader
import com.clairvoyant.data.scalaxy.test.util.matchers.DataFrameMatcher
import com.clairvoyant.data.scalaxy.test.util.readers.DataFrameReader
import org.apache.spark.sql.types.*

class JSONTextToDataFrameReaderSpec extends DataFrameReader with DataFrameMatcher {

  val jsonTextToDataFrameReader = TextToDataFrameReader[JSONTextFormat]

  "read() - with json text having array of records" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|[
         |  {
         |     "col1": "A1", 
         |     "col2": "A2"
         |  },
         |  {
         |     "col1": "B1", 
         |     "col2": "B2"
         |  }
         |]""".stripMargin

    val jsonTextFormat = JSONTextFormat()

    val df = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:string,col2:string>"
  }

  "read() - with json text having single record" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   "col1": "A1", 
         |   "col2": "A2"
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat()

    val df = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    df.count() shouldBe 1
    df.schema.simpleString shouldBe "struct<col1:string,col2:string>"
  }

  "read() - with json text having array of records in a single line" should "return a dataframe with correct count and schema" in {
    val jsonText = """[{"col1": "A1", "col2": "A2"},{"col1": "B1", "col2": "B2"}]""".stripMargin

    val jsonTextFormat = JSONTextFormat()

    val df = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:string,col2:string>"
  }

  "read() - with json text and allowBackslashEscapingAnyCharacter" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   "col1": "A1", 
         |   "col2": "A2",
         |   "col3": "\A3"
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      allowBackslashEscapingAnyCharacter = true
    )

    val actualDF = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    val expectedDF = readJSONFromText(
      """|{
         |   "col1": "A1", 
         |   "col2": "A2",
         |   "col3": "A3"
         |}""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with json text and allowComments" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|[
         | {
         |   "col1": "A1", 
         |   "col2": "A2",
         |   "col3": "A3"
         | },
         | // This is a comment
         | {
         |   "col1": "B1", 
         |   "col2": "B2",
         |   "col3": "B3"
         | }
         |]""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      allowComments = true
    )

    val actualDF = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    val expectedDF = readJSONFromText(
      """|[
         | {
         |   "col1": "A1", 
         |   "col2": "A2",
         |   "col3": "A3"
         | },
         | {
         |   "col1": "B1", 
         |   "col2": "B2",
         |   "col3": "B3"
         | }
         |]""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with json text and allowNonNumericNumbers" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   "col1": 1, 
         |   "col2": 2,
         |   "col3": NaN
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      allowNonNumericNumbers = true
    )

    val df = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    df.schema.simpleString shouldBe "struct<col1:bigint,col2:bigint,col3:double>"
  }

  "read() - with json text and allowNumericLeadingZeros" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   "col1": 1, 
         |   "col2": 2,
         |   "col3": 0003
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      allowNumericLeadingZeros = true
    )

    val actualDF = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    val expectedDF = readJSONFromText(
      """|{
         |   "col1": 1, 
         |   "col2": 2,
         |   "col3": 3
         |}""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with json text and allowSingleQuotes" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   'col1': 'A1', 
         |   'col2': 'A2',
         |   'col3': 'A3'
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      allowSingleQuotes = true
    )

    val actualDF = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    val expectedDF = readJSONFromText(
      """|{
         |   "col1": "A1", 
         |   "col2": "A2",
         |   "col3": "A3"
         |}""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with json text and allowUnquotedFieldNames" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   col1: "A1", 
         |   col2: "A2",
         |   col3: "A3"
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      allowUnquotedFieldNames = true
    )

    val actualDF = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    val expectedDF = readJSONFromText(
      """|{
         |   "col1": "A1", 
         |   "col2": "A2",
         |   "col3": "A3"
         |}""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with json text and dropFieldIfAllNull" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|[
         | {
         |   "col1": "A1", 
         |   "col2": null,
         |   "col3": "A3"
         | },
         | {
         |   "col1": "B1", 
         |   "col2": null,
         |   "col3": "B3"
         | }
         |]""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      dropFieldIfAllNull = true
    )

    val actualDF = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    val expectedDF = readJSONFromText(
      """|[
         | {
         |   "col1": "A1", 
         |   "col3": "A3"
         | },
         | {
         |   "col1": "B1", 
         |   "col3": "B3"
         | }
         |]""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with json text and prefersDecimal" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   "col1": 1, 
         |   "col2": 2,
         |   "col3": 3.0
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      prefersDecimal = true
    )

    val df = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    df.schema.simpleString shouldBe "struct<col1:bigint,col2:bigint,col3:decimal(2,1)>"
  }

  "read() - with json text and primitivesAsString" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   "col1": 1, 
         |   "col2": 2,
         |   "col3": 3.0
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat(
      primitivesAsString = true
    )

    val df = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat
    )

    df.schema.simpleString shouldBe "struct<col1:string,col2:string,col3:string>"
  }

  "read() - with json text and adaptSchemColumns" should "return a dataframe with correct count and schema" in {
    val jsonText =
      """|{
         |   "col1": 1, 
         |   "col2": 2,
         |   "col3": 3
         |}""".stripMargin

    val jsonTextFormat = JSONTextFormat()

    val actualDF = jsonTextToDataFrameReader.read(
      text = jsonText,
      textFormat = jsonTextFormat,
      adaptSchemaColumns =
        schema =>
          StructType(schema.map {
            case field @ StructField(_, dataType, _, _) if dataType == LongType =>
              field.copy(dataType = StringType)
            case field =>
              field
          })
    )

    val expectedDF = readJSONFromText(
      """|{
         |   "col1": "1", 
         |   "col2": "2",
         |   "col3": "3"
         |}""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

}
