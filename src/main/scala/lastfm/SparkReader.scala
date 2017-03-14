package lastfm

import Resolver._

/**
  * Created by davidsabater on 07/03/2017.
  */

object SparkReader extends Context {

  def main(args: Array[String]): Unit = {
    runWithSpark(loadDataResolve(args(0)))
  }

}
