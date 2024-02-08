package com.clairvoyant.data.scalaxy.reader.excel

import com.clairvoyant.data.scalaxy.test.util.matchers.DataFrameMatcher
import com.clairvoyant.data.scalaxy.test.util.readers.DataFrameReader

import java.io.FileInputStream
import scala.util.Using

class ExcelToDataFrameReaderSpec extends DataFrameReader with DataFrameMatcher {

  "read() - with excel filepath" should "return a dataframe with correct count and schema" in {

    val expectedDF = readJSONFromText(
      """
        |  [
        |    {
        |      "Created": "2021-07-29 10:35:12",
        |      "Advertiser": "Zola",
        |      "Transaction ID": "1210730000580100000",
        |      "Earnings": "$0.68",
        |      "SID": "wlus9",
        |      "Status": "CONFIRMED",
        |      "ClickPage": "https://www.zola.com/"
        |    },
        |    {
        |      "Created": "2022-04-18 07:23:54",
        |      "Advertiser": "TradeInn",
        |      "Transaction ID": "1220419021230020000",
        |      "Earnings": "$12.48",
        |      "SID": "wles7",
        |      "Status": "CONFIRMED",
        |      "ClickPage": "https://www.tradeinn.com/"
        |    }
        |  ]
        |""".stripMargin
    )

    val file = new java.io.File("excel/src/test/resources/sample_data.xlsx")
    val byteArray: Array[Byte] =
      Using(new FileInputStream(file)) { fis =>
        val byteArray = new Array[Byte](file.length.toInt)
        fis.read(byteArray)
        byteArray
      }.get

    ExcelToDataFrameReader.read(
      byteArray,
      ExcelFormat(dataAddress = "'Transactions Report'!A2:G4")
    ) should matchExpectedDataFrame(expectedDF)
  }

}
