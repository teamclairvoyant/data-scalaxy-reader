package com.clairvoyant.data.scalaxy.reader.excel

import com.clairvoyant.data.scalaxy.test.util.matchers.DataFrameMatcher
import com.clairvoyant.data.scalaxy.test.util.readers.DataFrameReader

import java.io.FileInputStream
import scala.util.Using

class ExcelToDataFrameReaderSpec extends DataFrameReader with DataFrameMatcher {

  val excelToDataFrameReader: ExcelToDataFrameReader.type = ExcelToDataFrameReader

  "read() - with excel filepath" should "return a dataframe with correct count and schema" in {
    val file = new java.io.File("excel/src/test/resources/sample_data.xlsx")
    val byteArray: Array[Byte] = Using(new FileInputStream(file)) { fis =>
      val byteArray = new Array[Byte](file.length.toInt)
      fis.read(byteArray)
      byteArray
    }.get

    val df = excelToDataFrameReader.read(
      byteArray,
      ExcelFormat(dataAddress = "'Transactions Report'!A2:G4")
    )
    df.count() shouldBe 2
  }
}
