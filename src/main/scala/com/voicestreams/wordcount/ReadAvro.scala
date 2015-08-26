package com.voicestreams.wordcount

package com.voicestreams.app

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ReadAvro extends App {
  val sparkConf = new SparkConf().setAppName("Avro Reader Job")
    .setMaster("local[4]")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.load("src/main/resources/episodes.avro", "com.databricks.spark.avro")

  df.registerTempTable("lines")
  sqlContext.sql("select title, doctor from lines where doctor > 5 order by doctor asc limit 5").show(5)
}

