package lastfm

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.format.ISODateTimeFormat

import scalaz.effect._

/**
  * Created by davidsabater on 10/03/2017.
  */
trait IOComponent {
  def parseTimeStamps(timestamp: String): Long = ISODateTimeFormat.dateTimeNoMillis.parseDateTime(timestamp).getMillis

  def loader(sc: SparkContext, file: String): RDD[ListenedSongs] = {
    sc.textFile(file)
      .map(_.split("\t"))
      .map(event => ListenedSongs(event(0), parseTimeStamps(event(1)), Song(event(3), event(2))))
  }

  def write[A, B] (map: Map[A, B], file: String) = IO {
    def userHomeDirectory = System.getProperty("user.home")
    val writer = new PrintWriter(new File(userHomeDirectory + "/" + file))
    for {
      tuple <- map
    }
      yield writer.write(tuple.toString() + "\n")
    writer.close()
  }
}
