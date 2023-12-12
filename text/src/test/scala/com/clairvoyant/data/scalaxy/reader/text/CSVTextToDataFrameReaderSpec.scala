package com.clairvoyant.data.scalaxy.reader.text

import com.clairvoyant.data.scalaxy.reader.text.formats.CSVTextFormat
import com.clairvoyant.data.scalaxy.reader.text.instances.CSVTextToDataFrameReader
import com.clairvoyant.data.scalaxy.test.util.SparkUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.*

class CSVTextToDataFrameReaderSpec extends SparkUtil {

  val csvTextToDataFrameReader = TextToDataFrameReader[CSVTextFormat]

  "read() - with csv text and headers" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|col1,col2
         |1,2
         |3,4
         |""".stripMargin

    val csvTextFormat = CSVTextFormat()

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:int,col2:int>"
  }

  "read() - with csv text and inferSchema as false" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|col1,col2
         |1,2
         |3,4
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      inferSchema = false
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:string,col2:string>"
  }

  "read() - with csv text and no headers" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|1,2
         |3,4
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      header = false
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<_c0:int,_c1:int>"
  }

  "read() - with csv text and semicolon as fields separator" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|col1;col2
         |1;2
         |3;4
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      sep = ";"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:int,col2:int>"
  }

  "read() - with csv text and semicolon as fields separator and # as records separator" should "return a dataframe with correct count and schema" in {
    val csvText = "col1;col2#1;2#3;4"

    val csvTextFormat = CSVTextFormat(
      sep = ";",
      recordSep = "#"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:int,col2:int>"
  }

  "read() - with csv text and semicolon as fields separator and # as records separator with no headers" should "return a dataframe with correct count and schema" in {
    val csvText = "1;2#3;4"

    val csvTextFormat = CSVTextFormat(
      header = false,
      sep = ";",
      recordSep = "#"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<_c0:int,_c1:int>"
  }

  "read() - with csv text and timestamp format" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|col1,col2
         |1,01-01-2023 00:00:00
         |3,02-01-2023 00:00:00
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      timestampFormat = "dd-MM-yyyy HH:mm:ss"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:int,col2:timestamp>"
  }

  "read() - with csv text and locale" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|col1,col2
         |1,27-MAI-1992
         |2,23-JUIN-1990
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      dateFormat = "dd-LLLL-yyyy",
      locale = "fr-FR"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
    df.schema.simpleString shouldBe "struct<col1:int,col2:date>"
  }

  "read() - with csv text and columnNameOfCorruptRecord" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|EmployeeID,EmployeeName,Address
         |1001,Arjun,Delhi
         |1002,Raj,Mumbai,Maharashtra
         |1003,Sunil,Hyderabad
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      columnNameOfCorruptRecord = "bad_record",
      mode = "PERMISSIVE"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 3

    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,Address:string,bad_record:string>"

    df.select("bad_record")
      .na
      .drop()
      .count() shouldBe 1

    df.select("bad_record")
      .na
      .drop()
      .collect()
      .map(_.getString(0)) shouldBe Array("1002,Raj,Mumbai,Maharashtra")
  }

  "read() - with csv text and escape character" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|"EmployeeID","EmployeeName","Address"
         |"1001","Arjun","Delhi"
         |"1002","Raj","Mumbai-#"Maharashtra#""
         |"1003","Sunil","Hyderabad"
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      escape = "#"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 3

    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,Address:string>"

    df.where(col("EmployeeID") === 1002)
      .select("Address")
      .collect()
      .map(_.getString(0)) shouldBe Array("Mumbai-\"Maharashtra\"")
  }

  "read() - with csv text and null value" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|EmployeeID,EmployeeName,Address
         |1001,Arjun,Delhi
         |1002,Raj,Invalid
         |1003,Sunil,Hyderabad
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      nullValue = "Invalid"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 3

    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,Address:string>"

    df.where(col("EmployeeID") === 1002)
      .select("Address")
      .collect()
      .head
      .getString(0) shouldBe null
  }

  "read() - with csv text and quote character" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|~EmployeeID~,~EmployeeName~,~Address~
         |~1001~,~Arjun~,~Delhi~
         |~1002~,~Raj~,~Mumbai~
         |~1003~,~Sunil~,~Hyderabad~
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      quote = "~"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 3
    assert(df.schema.simpleString != "#EmployeeID#:int,#EmployeeName#:string,#Address#:string")
    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,Address:string>"
  }

  "read() - with csv text and enforceSchema" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|EmployeeID,EmployeeName,Address
         |1001,Arjun,Delhi
         |1002,Raj,Mumbai
         |1003,Sunil,Hyderabad
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      enforceSchema = true
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat,
      originalSchema = Some(
        StructType(
          List(
            StructField("EmployeeID", IntegerType, nullable = false),
            StructField("EmployeeName", StringType, nullable = false),
            StructField("EmployeeAddress", StringType, nullable = false)
          )
        )
      )
    )

    df.count() shouldBe 3
    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,EmployeeAddress:string>"
  }

  "read() - with csv text and ignoreLeadingWhiteSpace as true" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|EmployeeID,EmployeeName,Address
         |1001,Arjun,Delhi
         |1002,  Raj,Mumbai
         |1003,Sunil,Hyderabad
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      ignoreLeadingWhiteSpace = true
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 3

    df.where(col("EmployeeID") === 1002)
      .select("EmployeeName")
      .collect()
      .head
      .getString(0) shouldBe "Raj"

    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,Address:string>"
  }

  "read() - with csv text and ignoreTrailingWhiteSpace as true" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|EmployeeID,EmployeeName,Address
         |1001,Arjun,Delhi
         |1002,Raj  ,Mumbai
         |1003,Sunil,Hyderabad
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      ignoreTrailingWhiteSpace = true
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 3

    df.where(col("EmployeeID") === 1002)
      .select("EmployeeName")
      .collect()
      .head
      .getString(0) shouldBe "Raj"

    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,Address:string>"
  }

  "read() - with csv text and nanValue" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|EmployeeID,EmployeeName,DeptID
         |1001,Arjun,1
         |1002,Raj,2
         |1003,Sunil,NA
         |1004,Shekhar,3
         |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      nanValue = "NA"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 4

    df.schema.simpleString shouldBe "struct<EmployeeID:int,EmployeeName:string,DeptID:double>"
  }

  "read() - with csv text and positiveInf" should "return a dataframe with correct count and schema" in {
    val csvText =
      """col1,col2
        |1,2
        |3,PositiveInfiniteValue""".stripMargin

    val csvTextFormat = CSVTextFormat(
      positiveInf = "PositiveInfiniteValue"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2

    df.schema.simpleString shouldBe "struct<col1:int,col2:double>"

    df.select("col2")
      .collect()
      .map(_.getDouble(0))
      .mkString(",") shouldBe "2.0,Infinity"
  }

  "read() - with csv text and negativeInf" should "return a dataframe with correct count and schema" in {
    val csvText =
      """col1,col2
        |1,2
        |3,NegativeInfiniteValue""".stripMargin

    val csvTextFormat = CSVTextFormat(
      negativeInf = "NegativeInfiniteValue"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2

    df.schema.simpleString shouldBe "struct<col1:int,col2:double>"

    df.select("col2")
      .collect()
      .map(_.getDouble(0))
      .mkString(",") shouldBe "2.0,-Infinity"
  }

  "read() - with csv text and comment" should "return a dataframe with correct count and schema" in {
    val csvText =
      """col1,col2
        |1,2
        |3,4
        |*5,6
        |""".stripMargin

    val csvTextFormat = CSVTextFormat(
      comment = "*"
    )

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat
    )

    df.count() shouldBe 2
  }

  "read() - with csv text and adaptSchemaColumns" should "return a dataframe with correct count and schema" in {
    val csvText =
      """|employee_id,employee_name,address
         |1001,Arjun,Delhi
         |1002,Raj,Mumbai
         |1003,Sunil,Hyderabad
         |""".stripMargin

    val csvTextFormat = CSVTextFormat()

    val df = csvTextToDataFrameReader.read(
      text = csvText,
      textFormat = csvTextFormat,
      adaptSchemaColumns =
        schema =>
          StructType(schema.map {
            case field @ StructField(_, dataType, _, _) if dataType == IntegerType =>
              field.copy(dataType = StringType)
            case field =>
              field
          })
    )

    df.count() shouldBe 3

    df.schema.simpleString shouldBe "struct<employee_id:string,employee_name:string,address:string>"
  }

}
