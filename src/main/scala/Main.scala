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

import java.io.{File, PrintWriter}
import scala.collection._


object Main {
  def main(args: Array[String]): Unit = {

//    System.setProperty("hadoop.home.dir", "c:/winutils/")

//    System.setProperty("hadoop.home.dir", "C:/hadoop")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Regression Project")
    .setMaster("local[4]")

    val sc = new SparkContext(conf)

//    val log = sc.textFile("access.log")
    val log = sc.textFile("access.log")

    val logLines = log.map(line => (line.split(" ")(3).trim().substring(1), line.split(" ")(0).trim))

    val result = logLines.groupByKey().map({case (time, ips) => (time, ips.size)}).sortBy(_._1)


//    result.saveAsTextFile("log-times");

    val pw = new PrintWriter(new File("log-times.csv"))
    val list = result.collect()
    list.foreach({case (time, num) => pw.write(time + ", " + num.toString + "\n")})

    pw.close()
  }
}

