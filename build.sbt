name := "SentimentAnalysis"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-analyzers-common" % "5.1.0",
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  "com.gravity" % "goose" % "2.1.23",
  "com.google.guava" % "guava" % "19.0"
)
    