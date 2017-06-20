package lastfm

import lastfm.SparkReader.Work
import org.apache.spark.rdd.RDD
import scala.concurrent.duration._
import scalaz.Reader

/**
  * Created by davidsabater on 09/03/2017.
  */
object Resolver extends IOComponent with Serializable{
  def loadDataResolve (file: String): Work[Any] =
    Reader(sc => {
      val rdd = loader(sc, file)
      write(resolvePartA(rdd), "PartA.txt").unsafePerformIO()
      write(resolvePartB(rdd), "PartB.txt").unsafePerformIO()
      write(resolvePartC(rdd, 20), "PartC.txt").unsafePerformIO()
    })

  def resolvePartA(rdd: RDD[ListenedSongs]): Map[String, Long] = {
    rdd.map(event => (event.userID, event.song))
      .distinct()
      .mapValues(_ => 1L)
      .reduceByKey(_ + _)
      .collectAsMap()
      .toMap
  }

  def resolvePartB(rdd: RDD[ListenedSongs]): Map[Song, Long] = {
    rdd.map(event => (event.song, 1L))
      .reduceByKey(_ + _)
      .takeOrdered(100)(Ordering[Long].reverse.on(x=>x._2))
      .toMap
  }

  def resolvePartC(rdd: RDD[ListenedSongs], MaxMinutes: Int): Map[String, List[Session]] = {
    rdd.map(event => (event.userID, (event.song, event.timestamp)))
      .groupByKey()
      .mapValues(_.toList.sortBy(_._2)
        .foldLeft(List[(Session)]()){
          case (List(), track) => List(Session(track._2, track._2, 0, List(track)))
          case (list, track) if Math.abs(track._2 - list.last.startTime) <= MaxMinutes.minutes.toMillis => {
            // We add track to existing session
            list.init :+ Session(list.last.startTime, track._2, track._2 - list.last.startTime, list.last.songs :+ track)
          }
          case (list, track) if Math.abs(track._2 - list.last.startTime) > MaxMinutes.minutes.toMillis =>  {
            // We start a new session
            list :+ Session(track._2, track._2, track._2 - list.last.startTime, List(track))
          }
      })
      .top(10)(Ordering.by(_._2.foreach(session => session.duration)))
      .toMap
  }
}
