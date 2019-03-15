package com.edu.spark.text

import org.apache.spark.sql.DataFrameReader


object ReaderObject {
  implicit class UDFTextReader(val reader: DataFrameReader) extends AnyVal{
    def udftext(path:String) = reader.format("udftext").load(path)
  }
}
