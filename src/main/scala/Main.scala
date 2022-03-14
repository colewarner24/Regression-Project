import breeze.linalg.Axis._1
import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.execution.streaming.state
import org.json4s.scalap.scalasig.ClassFileParser.state
import spire.math.Polynomial.x

import scala.collection._


object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Regression Project")
    .setMaster("local[4]")

    val sc = new SparkContext(conf)

    val log = sc.textFile("access.log")
//    val log = sc.textFile("test")

    val logLines = log.map(line => (line.split(" ")(3).trim(), line.split(" ")(0).trim))

    val result = logLines.groupByKey().map({case (time, ips) => (time, ips.size)}).sortBy(_._1)


    result.foreach(println)




  }
}

