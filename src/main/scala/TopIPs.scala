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


object TopIPs {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("Regression Project")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    val log = sc.textFile("test")

    val logLines = log.map(line => (line.split(" ")(3).split(":")(0).trim().substring(1), (line.split(" ")(0).trim)))

//    val result = logLines.groupByKey().map({case (time, ips) => (time, ips.size)}).sortBy(_._1)

    val result = logLines.groupByKey().map({case (date, ips) => (date, ips.toList.groupBy(identity).map(x => (x._1, x._2.length)).toList.sortBy(_._2).reverse.take(5))
    }).sortByKey().collect()



//    result.foreach(println)
    val pw = new PrintWriter(new File("top_ips.csv"))
    result.foreach({case (date, ips) => pw.write(date + ", " + ips.mkString(", ") + "\n")})

    pw.close()
  }

}
