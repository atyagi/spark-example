name := "spark-predict-income-example"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.0"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.3.0"
libraryDependencies += "joda-time" % "joda-time" % "2.9.2"
