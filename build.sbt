name := "StaticBasedOptim"

version := "0.1"

scalaVersion := "2.11.7"

// Spark
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.2.1"

// Jena
libraryDependencies += "org.apache.jena" % "jena-core" % "3.0.1"
libraryDependencies += "org.apache.jena" % "jena-arq" % "3.0.1"