package Static

import Static.LUBM.Query14
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

/*  val q1 = Query1(sc)
  println("Q1:" +q1)
  val q2 = Query2(sc)
  println("Q2:" +q2)
  val q3 = Query3(sc)
  println("Q3:" +q3)
  val q4 = Query4(sc)
  println("Q4:" + q4)
  val q5 = Query5(sc)
  println("Q5:" + q5)
  val q6 = Query6(sc)
  println("Q6:" + q6)
  val q7 = Query7(sc)
  println("Q7:" + q7)
  val q8 = Query8(sc)
  println("Q8:" + q8)
  val q9= Query9(sc)
  println("Q9:" + q9)
  val q10= Query10(sc)
  println("Q10:" + q10)
  val q11= Query11(sc)
  println("Q11:" + q11)///!!!
  val q12= Query12(sc)
  println("Q12:" + q12)
  val q13= Query13(sc)
  println("Q13:" + q13)/// !!!*/
  val q14= Query14(sc)
  println("Q14:" + q14)
  //   println("Triplet Query 1*************************************")
 // StaticBasedOptim.print()
  println("--------------------------------------------")
}
