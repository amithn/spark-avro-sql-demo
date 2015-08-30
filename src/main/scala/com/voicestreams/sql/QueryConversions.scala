package com.voicestreams.sql

package com.voicestreams.spark.sql

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.Path


object QueryConversions extends App {

  case class Book(title: String, age: Int)

  Path("./saved").deleteRecursively()

  val sparkConf = new SparkConf().setAppName("Avro Reader Job").setMaster("local[4]")

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  //val df = sqlContext.load("src/main/resources/episodes.avro", "com.databricks.spark.avro")
  val df = sqlContext.load("/Users/voicestreams/testbed/visualsessions/part-00000.avro", "com.databricks.spark.avro")

  import com.voicestreams.spark.sql.QueryConversions.sqlContext.implicits._

  var conversionsDF = df.select("pages.conversions")
  conversionsDF.printSchema()

  for(column <- df.columns) {
    println(column)
  }

  case class Conversion(id : String, name : String)

  var conversion = Conversion("","")

  def convert(array : ArrayBuffer[_]) : Array[Conversion] = {
    val conversions = ArrayBuffer[Conversion]()
    if(array.length > 0) {
      for (elem <- array) {
        val row = elem.asInstanceOf[ArrayBuffer[GenericRowWithSchema]]
        //        println("Element is " + elem)
        for (col <- row) {
          //          println("Col is id" + col(0) + " value " + col(1))
          conversions += Conversion(col(0).asInstanceOf[String], col(1).asInstanceOf[String])
        }
      }
    }
    return conversions.toArray
  }

  def filterRowsWithNoData(row : ArrayBuffer[ArrayBuffer[_]]): ArrayBuffer[_] = {
    val arr = row.filter( x => x.size > 0)
    return arr
  }

  val conversions = conversionsDF.map(x => filterRowsWithNoData(x(0).asInstanceOf[ArrayBuffer[ArrayBuffer[_]]]))
    .filter( x => x.size > 0)
    .flatMap( x => convert(x))
    .filter( x => x.name != null)

  conversionsDF = conversions.toDF
  conversionsDF.registerTempTable("conversions")
  sqlContext.sql("select id, name from conversions").show(2000)
  sqlContext.sql("select id, count(*) as total from conversions group by id order by total desc").show(100)
  sqlContext.sql("select id, name  from conversions where name like '%porn%'").show(100)

}


