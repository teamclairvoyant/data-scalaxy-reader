package com.clairvoyant.data.scalaxy.reader.excel

import zio.config.derivation.nameWithLabel

@nameWithLabel
case class ExcelFormat(
    header: Boolean = true,
    dataAddress: String = "A1",
    treatEmptyValuesAsNulls: Boolean = true,
    setErrorCellsToFallbackValues: Boolean = false,
    usePlainNumberFormat: Boolean = false,
    inferSchema: Boolean = false,
    addColorColumns: Boolean = false,
    timestampFormat: String = "yyyy-mm-dd hh:mm:ss",
    excerptSize: Int = 10,
    maxRowsInMemory: Option[Long] = None,
    maxByteArraySize: Option[Long] = None,
    tempFileThreshold: Option[Long] = None
)
