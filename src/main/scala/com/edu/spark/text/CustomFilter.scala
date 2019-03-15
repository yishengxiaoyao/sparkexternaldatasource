package com.edu.spark.text

import com.edu.spark.util.Util
import org.apache.spark.sql.types.StructType

case class CustomFilter(attr : String, value : Any, filter : String)
object CustomFilter {
  def applyFilters(filters : List[CustomFilter], value : String, schema : StructType): Boolean = {
    var includeInResultSet = true

    val schemaFields = schema.fields
    val index = schema.fieldIndex(filters.head.attr)
    val dataType = schemaFields(index).dataType
    val castedValue = Util.castTo(value, dataType)

    filters.foreach(f => {
      val givenValue = Util.castTo(f.value.toString, dataType)
      f.filter match {
        case "equalTo" => {
          includeInResultSet = castedValue == givenValue
          println("custom equalTo filter is used!!")
        }
        case "greaterThan" => {
          includeInResultSet = castedValue.equals(givenValue)
          println("custom greaterThan filter is used!!")
        }
        case _ => throw new UnsupportedOperationException("this filter is not supported!!")
      }
    })

    includeInResultSet
  }
}
