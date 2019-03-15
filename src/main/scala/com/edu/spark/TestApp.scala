package com.edu.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.edu.spark.text.ReaderObject.UDFTextReader

object TestApp extends App {
  println("Application Started...")
  val conf=new SparkConf().setAppName("spark-custom-datasource")
  val spark=SparkSession.builder().config(conf).master("local[2]").getOrCreate()
  //val df=spark.sqlContext.read.format("udftext").load("/Users/renren/Downloads/data")
  val df=spark.sqlContext.read.udftext("/Users/renren/Downloads/data")
  //println("output schema...")
  //df.printSchema()
  df.show()
  //save the data
  //df.write.options(Map("format" -> "customFormat")).mode(SaveMode.Overwrite).format("com.edu.spark.text").save("/Users//Downloads/out_custom/")
  //df.write.options(Map("format" -> "json")).mode(SaveMode.Overwrite).format("com.edu.spark.text").save("/Users//Downloads/out_json/")
  //df.write.mode(SaveMode.Overwrite).format("com.edu.spark.text").save("/Users//Downloads/out_none/")
  //select some specific columns
  //df.createOrReplaceTempView("test")
  //spark.sql("select id, name, salary from test").show()
  //filter data
  //df.createOrReplaceTempView("test")
  //spark.sql("select id,name,gender from test where salary == 50000").write.mode(SaveMode.Overwrite).format("com.edu.spark.text").save("/Users//Downloads/data/result")
  //spark.sql("select id,name,gender from test where salary == 50000").show()
  println("Application Ended...")
}
