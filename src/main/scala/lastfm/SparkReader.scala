package lastfm

import org.apache.spark.{SparkConf, SparkContext}

import scalaz.Reader

/**
  * Created by davidsabater on 07/03/2017.
  */
object SparkReader {
  type Work[A] = Reader[SparkContext, A]

  def runWithSpark[T](work: Work[T]): T = {
    val processorCount = Runtime.getRuntime.availableProcessors()
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("reader")
      .setMaster(s"local[$processorCount]")

    val sc = new SparkContext(sparkConf)

    try {
      work.run(sc)
    } finally {
      sc.stop
    }
  }

}
