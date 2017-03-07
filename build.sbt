name := "spark-reader-lastfm"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.scalaz" %% "scalaz-core" % "7.1.1"
)