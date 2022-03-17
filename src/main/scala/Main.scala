import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}

import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.rdd._

import java.time.LocalDate
import org.datanucleus.store.types.simple.Date
import java.time.ZoneId
import java.time.ZonedDateTime

import java.io.{BufferedWriter, File, FileWriter}
import java.time.format.DateTimeFormatter
import java.util
import scala.collection.mutable
import scala.io.Source
import scala.collection._
import scala.collection.mutable.ListBuffer

object Main {
  def main (args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    finalProject()
  }
  def finalProject(): Unit ={
    val conf = new SparkConf().setAppName("App").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("./src/main/ProjectData/access.log")

    val FIRST_SECONDS = parseSecondsOutOfLine(data.first())

    /*
                |
      # of      |
      requests  |
                |
                |_________________
                     time (s)
     */

    // (183540,8)
    // In second 183540, 8 requests were made
    val requestsAtEachSecond =
    data
      .map(line => parseSecondsOutOfLine(line) - FIRST_SECONDS) // map to seconds (from the first request log)
      .groupBy(s => s) // aggregate all requests in 1 second
      .map({case (seconds, compactbuffer) => (seconds, compactbuffer.toList.length)}) // attach # of requests to seconds

    val n = (requestsAtEachSecond.collect().length * 1.0)

    val xBar = requestsAtEachSecond
      .reduce((tuple1, tuple2) => {
        (tuple1._1 + tuple2._1, 0)
      })._1 * 1.0 / n

    val yBar = requestsAtEachSecond
      .reduce((tuple1, tuple2) => {
        (0, tuple1._2 + tuple2._2)
      })._2 * 1.0 / n

    val mNumerator = requestsAtEachSecond
      .map({case (x, y) => {
        (x - xBar) * (y - yBar)
      }})
      .reduce((ai, aj) => ai + aj) * 1.0

    val mDenominator = requestsAtEachSecond
      .map({case (x, y) => {
        Math.pow(x - xBar, 2)
      }})
      .reduce((ai, aj) => ai + aj) * 1.0

    val m = mNumerator / mDenominator

    val b = yBar - m * xBar

    println(m)
  }
  def parseSecondsOutOfLine(line: String): Long ={
    val dateString = line.slice(line.indexOf("[") + 1, line.indexOf("+") - 1)
    val formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss")
    val ldt = LocalDateTime.parse(dateString, formatter)

    val zdt = ldt.atZone(ZoneId.of("America/Los_Angeles"))
    zdt.toInstant.toEpochMilli / 1000
  }
}