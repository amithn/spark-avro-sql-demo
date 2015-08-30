package com.voicestreams.spark.sql

import com.databricks.spark.avro._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ReadAvro extends App {

  case class Book(title: String, age: Int)

  val sparkConf = new SparkConf().setAppName("Avro Reader Job")
    .setMaster("local[4]")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.load("src/main/resources/episodes.avro", "com.databricks.spark.avro")


  val rdd = df.map(x => Book(x(0).asInstanceOf[String], x(2).asInstanceOf[Int]))
  rdd.foreach(println)

  df.registerTempTable("lines")

  for(column <- df.columns) {
    println(column)
  }

  val dfFinal = sqlContext.sql("select title,doctor from lines where doctor > 5 order by doctor asc limit 5")
  df.saveAsAvroFile("saved")
}

