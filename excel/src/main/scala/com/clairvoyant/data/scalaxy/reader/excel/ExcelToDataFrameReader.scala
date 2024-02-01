package com.clairvoyant.data.scalaxy.reader.excel

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{ByteArrayInputStream, File, FileOutputStream, PrintWriter}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

implicit object ExcelToDataFrameReader {

  def read(
      bytes: Array[Byte],
      excelFormat: ExcelFormat,
      originalSchema: Option[StructType] = None,
      adaptSchemaColumns: StructType => StructType = identity
  ) (using sparkSession: SparkSession): DataFrame =

    import sparkSession.implicits.*

    def saveBytesToTempExcelFiles(bytes: Array[Byte]) = {
      val workbook = new XSSFWorkbook(new ByteArrayInputStream(bytes))

      val file = File.createTempFile("excel-data-", ".xlsx")
      file.deleteOnExit()
      val fileOut = new FileOutputStream(file)
      new PrintWriter(file) {
        try {
          workbook.write(fileOut)
        } finally {
          close()
        }
      }
      file
    }

    val tempExcelFile = saveBytesToTempExcelFiles(bytes)

    val excelDataFrameReader = sparkSession.read
      .format("com.crealytics.spark.excel")
      .options(
        Map(
          "header" -> excelFormat.header,
          "dataAddress" -> excelFormat.dataAddress,
          "treatEmptyValuesAsNulls" -> excelFormat.treatEmptyValuesAsNulls,
          "setErrorCellsToFallbackValues" -> excelFormat.setErrorCellsToFallbackValues,
          "usePlainNumberFormat" -> excelFormat.usePlainNumberFormat,
          "inferSchema" -> excelFormat.inferSchema,
          "addColorColumns" -> excelFormat.addColorColumns,
          "timestampFormat" -> excelFormat.timestampFormat,
          "excerptSize" -> excelFormat.excerptSize
        ).map((optionName, optionValue) => (optionName, optionValue.toString))
      )
      .options(
        Map(
          "maxRowsInMemory" -> excelFormat.maxRowsInMemory,
          "maxByteArraySize" -> excelFormat.maxByteArraySize,
          "tempFileThreshold" -> excelFormat.tempFileThreshold,
        ).collect {
          case (optionName, Some(optionValue)) => (optionName, optionValue.toString)
        }
      )

    excelDataFrameReader
      .schema {
        originalSchema.getOrElse {
          adaptSchemaColumns {
            excelDataFrameReader
              .load(tempExcelFile.getAbsolutePath)
              .schema
          }
        }
      }
      .load(tempExcelFile.getAbsolutePath)
}
