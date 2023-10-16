package com.clairvoyant.data.scalaxy.reader.text

import com.clairvoyant.data.scalaxy.reader.text.TextToDataFrameReader
import com.clairvoyant.data.scalaxy.reader.text.formats.{CSVTextFormat, HTMLTableTextFormat}
import com.clairvoyant.data.scalaxy.reader.text.instances.HTMLTableTextToDataFrameReader
import com.clairvoyant.data.scalaxy.test.util.SparkUtil
import com.clairvoyant.data.scalaxy.test.util.matchers.DataFrameMatcher
import com.clairvoyant.data.scalaxy.test.util.readers.DataFrameReader
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.*

class HTMLTableTextToDataFrameReaderSpec extends DataFrameReader with DataFrameMatcher {

  "read() - with html text" should "return a dataframe with correct count and schema" in {
    val htmlText =
      """
      <!DOCTYPE html>
      <div class="ac-card-content ac-card-content-normal ac-card-content-normal-alt">
          <p class="a-spacing-base ac-cms-promo-big"><img alt="" src="https://images-eu.ssl-images-amazon.com/images/G/02/associates/network/holborn/ads-promo._V285961827_.png">
              Are you monetising international traffic? You can now receive earnings in an international bank account. <a href="https://amazon-affiliate.eu/en/receive-international-earnings/" target="_blank">Click here</a> to
              learn more.
          </p>
          <div class="a-dtt-datatable">
              <table class="a-dtt-table">
                  <thead class="a-dtt-thead">
                  <tr>
                      <th class="a-dtt-header">
                          Date
                      </th>
                      <th class="a-dtt-header">
                          Transaction
                      </th>
                      <th class="a-dtt-header">
                          <div class="ac-float-r">
                              Amount
                          </div>
                      </th>
                      <th class="a-dtt-header">
                          <div class="ac-float-r">
                              Balance
                          </div>
                      </th>
                  </tr>
                  </thead>
                  <tbody class="a-dtt-tbody">
                  <tr>
                      <td>
                          01/06/2022
                      </td>
                      <td>
                          04/2022 Advertising Fees
                      </td>
                      <td>
                          <div class="ac-float-r">
                              £4,049.19
                          </div>
                      </td>
                      <td>
                          <div class="ac-float-r">
                              £4,049.19
                          </div>
                      </td>
                  </tr>
                  <tr>
                      <td>
                          30/05/2022
                      </td>
                      <td>
                          Payment by Direct Deposit
                      </td>
                      <td>
                          <div class="ac-float-r">
                              <div class="ac-payment-balance-negative">
                                  -£3,349.17
                              </div>
                          </div>
                      </td>
                      <td>
                          <div class="ac-float-r">
                              £0.00
                          </div>
                      </td>
                  </tr>
                  </tbody>
              </table>
          </div>
          <div class="ac-payment-note ac-standard-alert">
              <div class="a-box a-alert a-alert-info" aria-live="polite" aria-atomic="true">
                  <div class="a-box-inner a-alert-container"><i class="a-icon a-icon-alert"></i>
                      <div class="a-alert-content">Please note:
                          Any transactions that occurred prior to May 10, 2000, will not be visible on this report.
                      </div>
                  </div>
              </div>
          </div>
      </div>
      """.stripMargin

    val htmlTableTextFormat = HTMLTableTextFormat()

    val actualDF = TextToDataFrameReader.read(
      text = htmlText,
      textFormat = htmlTableTextFormat
    )

    val expectedDF = readCSVFromText(
      text =
        """
          |Amount~Balance~Date~Transaction
          |£4,049.19~£4,049.19~01/06/2022~04/2022 Advertising Fees
          |-£3,349.17~£0.00~30/05/2022~Payment by Direct Deposit
      """.stripMargin,
      csvOptions = Map(
        "sep" -> "~"
      )
    )

    actualDF should matchExpectedDataFrame(expectedDF)
  }

}
