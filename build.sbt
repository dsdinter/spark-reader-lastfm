name := "spark-reader-lastfm"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",
  "org.scalaz" %% "scalaz-core" % "7.2.9",
  "org.scalaz" %% "scalaz-effect" % "7.2.9"
)