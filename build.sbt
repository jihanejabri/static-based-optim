name := "StaticBasedOptim"

version := "0.1"

scalaVersion := "2.11.7"

//Spark
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"%"provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.10" % "1.5.2"