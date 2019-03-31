package Static

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  println("test")

  System.setProperty("hadoop.home.dir", "C:\\winutils\\")

  val query = "SELECT ?x WHERE { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> }"
  val folder = "data"

  val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import sqlContext.implicits._

  Logger.getRootLogger.setLevel(Level.WARN)
  val d = Dct(sc, folder)

  val f = Query1(sc)
  println("Q1:" +f)

//  println("Triplet Query 1*************************************")
 // StaticBasedOptim.print()
  println("--------------------------------------------")
}
