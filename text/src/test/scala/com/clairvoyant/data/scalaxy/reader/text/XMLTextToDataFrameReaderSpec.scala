package com.clairvoyant.data.scalaxy.reader.text

import com.clairvoyant.data.scalaxy.reader.text.formats.XMLTextFormat
import com.clairvoyant.data.scalaxy.reader.text.instances.XMLTextToDataFrameReader
import com.clairvoyant.data.scalaxy.test.util.matchers.DataFrameMatcher
import com.clairvoyant.data.scalaxy.test.util.readers.DataFrameReader
import org.apache.spark.sql.types.*

class XMLTextToDataFrameReaderSpec extends DataFrameReader with DataFrameMatcher {

  "read() - with xml text" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<row>
         |  <col1>A1</col1>
         |  <col2>A2</col2>
         |  <col3>A3</col3>
         |</row>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat()

    val df = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    df.count() shouldBe 1
    df.schema.simpleString shouldBe "struct<col1:string,col2:string,col3:string>"
  }

  "read() - with xml text and attributePrefix" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<row type="x">
         |  <col1>A1</col1>
         |  <col2>A2</col2>
         |  <col3>A3</col3>
         |</row>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      attributePrefix = "attr_"
    )

    val actualDF = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    val expectedDF = readJSONFromText(
      """|{
         |  "attr_type": "x",
         |  "col1": "A1",
         |  "col2": "A2",
         |  "col3": "A3"
         |}
         |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with xml text and columnNameOfCorruptRecord" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<?xml version="1.0"?>
         |<ROWSET>
         |    <ROW>
         |        <year>2012</year>
         |        <make>Tesla</make>
         |        <model>>S
         |        <comment>No comment</comment>
         |    </ROW>
         |    <ROW>
         |        </year>
         |        <make>Ford</make>
         |        <model>E350</model>model></model>
         |        <comment>Go get one now they are going fast</comment>
         |    </ROW>
         |    <ROW>
         |        <year>2015</year>
         |        <make>Chevy</make>
         |        <model>Volt</model>
         |    </ROW>
         |</ROWSET>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      columnNameOfCorruptRecord = "_malformed_records",
      mode = "PERMISSIVE",
      rowTag = "ROW"
    )

    val df = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    val cars = df.collect()
    assert(cars.length === 3)

    val malformedRowOne = df.select("_malformed_records").first().get(0).toString
    val malformedRowTwo = df.select("_malformed_records").take(2).last.get(0).toString
    val expectedMalformedRowOne =
      "<ROW><year>2012</year><make>Tesla</make><model>>S" +
        "<comment>No comment</comment></ROW>"
    val expectedMalformedRowTwo =
      "<ROW></year><make>Ford</make><model>E350</model>model></model>" +
        "<comment>Go get one now they are going fast</comment></ROW>"

    assert(malformedRowOne.replaceAll("\\s", "") === expectedMalformedRowOne.replaceAll("\\s", ""))
    assert(malformedRowTwo.replaceAll("\\s", "") === expectedMalformedRowTwo.replaceAll("\\s", ""))
    assert(cars(2)(0) === null)
    assert(cars(0).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(1).toSeq.takeRight(3) === Seq(null, null, null))
    assert(cars(2).toSeq.takeRight(3) === Seq("Chevy", "Volt", 2015))
  }

  "read() - with xml text and excludeAttribute" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<row type="x">
         |  <col1>A1</col1>
         |  <col2>A2</col2>
         |  <col3>A3</col3>
         |</row>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      excludeAttribute = true
    )

    val actualDF = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    val expectedDF = readJSONFromText(
      """|{
         |  "col1": "A1",
         |  "col2": "A2",
         |  "col3": "A3"
         |}
         |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with xml text and nullValue" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<row>
         |  <col1>A1</col1>
         |  <col2></col2>
         |  <col3>A3</col3>
         |</row>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      nullValue = ""
    )

    val actualDF = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    val expectedDF = readJSONFromText(
      """|{
         |  "col1": "A1",
         |  "col2": null,
         |  "col3": "A3"
         |}
         |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with xml text and rowTag" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<cols>
         |  <col1>A1</col1>
         |  <col2>A2</col2>
         |  <col3>A3</col3>
         |</cols>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      rowTag = "cols"
    )

    val df = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    df.count() shouldBe 1
    df.schema.simpleString shouldBe "struct<col1:string,col2:string,col3:string>"
  }

  "read() - with xml text and ignoreSurroundingSpaces" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<people>
         |  <person>
         |    <age born=" 1990-02-24 "> 25 </age>
         |    <name>
         |
         |       Hyukjin
         |
         |    </name>
         |  </person>
         |  <person>
         |    <age born="
         |
         |    1985-01-01">   30 </age>
         |    <name>Lars</name>
         |  </person>
         |  <person>
         |    <age born=" 1980-01-01">30 </age>
         |    <name> Lion </name>
         |  </person>
         |</people>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      ignoreSurroundingSpaces = true,
      rowTag = "person"
    )

    val df = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    val results = df.collect()

    val attrValOne = results(0).getStruct(0)(1)
    val attrValTwo = results(1).getStruct(0)(0)

    assert(attrValOne.toString === "1990-02-24")
    assert(attrValTwo === 30)
    assert(results.length === 3)
  }

  "read() - with xml text and ignoreNamespace" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<?xml version="1.0"?>
         |<catalog xmlns:abc="abc" xmlns:def="def">
         |   <book abc:id="bk101" >
         |      <abc:author>Gambardella, Matthew</abc:author>
         |   </book>
         |   <book def:id="bk102">
         |      <def:author>Ralls, Kim</def:author>
         |   </book>
         |   <book id="bk103">
         |      <author>Corets, Eva</author>
         |   </book>
         |</catalog>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      ignoreNamespace = true,
      rowTag = "book"
    )

    val df = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    assert(df.filter("author IS NOT NULL").count() === 3)
    assert(df.filter("_id IS NOT NULL").count() === 3)
  }

  "read() - with xml text and valueTag" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<?xml version="1.0"?>
         |<catalog>
         |   <book id="bk102">
         |      <author>Ralls, Kim</author>
         |      <title>Midnight Rain</title>
         |      <price unit="$">5.95</price>
         |   </book>
         |</catalog>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat(
      rowTag = "book",
      valueTag = "#VALUE"
    )

    val actualDF = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat
    )

    val expectedDF = readJSONFromText(
      text =
        """|{
           |  "_id": "bk102",
           |  "author": "Ralls, Kim",
           |  "price": {
           |    "_unit": "$",
           |    "#VALUE": 5.95
           |  },
           |  "title": "Midnight Rain"
           |}
           |""".stripMargin
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

  "read() - with xml text and adaptSchemaColumns" should "return a dataframe with correct count and schema" in {
    val xmlText =
      """|<row>
         |  <col1>1</col1>
         |  <col2>2</col2>
         |  <col3>3</col3>
         |</row>
         |""".stripMargin

    val xmlTextFormat = XMLTextFormat()

    val df = TextToDataFrameReader.read(
      text = xmlText,
      textFormat = xmlTextFormat,
      adaptSchemaColumns =
        schema =>
          StructType(schema.map {
            case field @ StructField(_, dataType, _, _) if dataType == LongType =>
              field.copy(dataType = StringType)
            case field =>
              field
          })
    )

    df.count() shouldBe 1
    df.schema.simpleString shouldBe "struct<col1:string,col2:string,col3:string>"
  }

}
