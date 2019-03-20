package Static

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import  org.apache.spark.sql.DataFrame

class Dct(sc : SparkContext, dir : String) {
  var lubmEncoded = "/LUBMInstances/lubm1Encoded.txt/"
  var lubmConcept = "/univ-bench_conceptsURL2Id.txt"
  var lubmProperties = "/univ-bench_propertiesURL2Id.txt"
  var propertiesId2Range = "/univ-bench_propertiesId2Range.txt"
  var propertiesId2Domain = "/univ-bench_propertiesId2Domain.txt"

  //  val data = sc.textFile(dir +lubmEncoded).map(x=>x.split(" "))
  val lubmEncodedData = sc.textFile(dir +lubmEncoded)
  println("PRINTING DATA ----- = "+lubmEncodedData.take(14).foreach(println))
  //  println(data)
  val conceptId2URI = lubmEncodedData.map(x=>x.split(" ")).map(x=>(x(0), x(1), x(2)))
  //  conceptId2URI.persist()
  println("Data conceptId2URI count = "+conceptId2URI.count)
  println("conceptId2URI println= "+conceptId2URI.take(44).foreach(println))

  val lubmConceptData = sc.textFile(dir + lubmConcept)
  val lubmConceptData_ = lubmConceptData.map(x=>x.split("\t")).map(x=>(x(0), x(1),x(2), x(3)))
  //  conceptId2URII.persist()
  println("lubmConceptData_ count :"+lubmConceptData_.count)

  println("lubmConceptData_ println = "+lubmConceptData_.take(44).foreach(println))


  val lubmPropertiesData = sc.textFile(dir +lubmProperties)
  println("PRINTING DATA ----- = "+lubmPropertiesData.take(4).foreach(println))
  //  println(data)
  //  val lubmPropertiesData_ = lubmPropertiesData.map(x=>x.split("\t")).map(x=>(x(0), x(1), x(2), x(3), x(4), x(5)))
  val lubmPropertiesData_ = lubmPropertiesData.map(x=>x.split("\t")).map(x=>(x(0), x(1), x(2), x(3)))
  //  conceptId2URI.persist()
  println("Data conceptId2URI count = "+lubmPropertiesData_.count)
  println("conceptId2URI println= "+lubmPropertiesData_.take(4).foreach(println))

  val bench_propertiesId2RangeData = sc.textFile(dir + propertiesId2Range)
  println("PRINTING DATA ----- = "+bench_propertiesId2RangeData.take(4).foreach(println))

  val bench_propertiesId2RangeData_ = bench_propertiesId2RangeData.map(x=>x.split("\t")).map(x=>(x(0), x(1)))

  println("Data propertiesId2Range count = "+bench_propertiesId2RangeData_.count)
  println("propertiesId2Range println= "+bench_propertiesId2RangeData_.take(4).foreach(println))

  val bench_propertiesId2DomainData = sc.textFile(dir + propertiesId2Domain)
  println("PRINTING DATA ----- = "+bench_propertiesId2DomainData.take(4).foreach(println))

  val bench_propertiesId2DomainData_ = bench_propertiesId2DomainData.map(x=>x.split("\t")).map(x=>(x(0), x(1)))

  println("Data propertiesId2Domain count = "+bench_propertiesId2DomainData_.count)
  println("propertiesId2Domain println= "+bench_propertiesId2DomainData_.take(4).foreach(println))

  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val trilpesDF = conceptId2URI.toDF("subject", "predicat", "object")
  println("Triples = "+trilpesDF.count)
  println("trilpesDF println= "+trilpesDF.take(4).foreach(println))

  /****************************** QUERY 1 ************************/
  /*
    * PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ub:<http://www.univ-mlv.fr/~ocure/lubm.owl#>
  PREFIX owl:<http://www.w3.org/2002/07/owl#>
  PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  SELECT ?x ?y ?z WHERE {
    ?x rdf:type ub:GraduateStudent .
    ?y rdf:type ub:University .
    ?z rdf:type ub:Department .
    ?x ub:memberOf ?z .
    ?z ub:subOrganizationOf ?y .
    ?x ub:undergraduateDegreeFrom ?y.
  }
    * */
  val rdfs = "http://www.w3.org/2000/01/rdf-schema";
  val ub = "http://www.univ-mlv.fr/~ocure/lubm.owl";
  val owl = "http://www.w3.org/2002/07/owl";
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns";

  val query1 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> \n}"
  val query3 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication> . \n\t?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor> <http://www.Department0.University0.edu/AssistantProfessor0> \n}"
  val query5 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person> . \n\t?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf> <http://www.Department0.University0.edu> \n}"
  val query6 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> \n}"
  val query10 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> . \n\t?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse> <http://www.Department0.University0.edu/GraduateCourse0> \n}"
  val query14 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> \n}"


  val dict ="/lubm.ttl"

  /* val dataFrame = sqlContext.sparqlQuery(dict, query1)
  dataFrame.show()*/

  /*val dataFrame6 = sqlContext.sparqlQuery(dict, query6)
  dataFrame6.show()*/

/*
  val name = Map("rdfs" -> rdfs , "ub"-> ub, "owl"->owl, "rdf"->rdf)1

  def quer+yT(typ:String, prop: String): String = name.get(typ).get + "#" + prop
  //val typeOf = lubmConceptData_.lookup.queryT("", "").head._1

  val Student : (Long, Long) = lubmConceptData_.lookup.(name.get("ub").get + "#" + "Student".head)
  val s1 = Student._1
  val s2 = Student._2

 // def borne(concept : (Long, Byte, Byte)) : (Long, Long) = {
   // val length = 44
   // val id = concept._1
  //}
  */

}
object Dct {
  def apply(sc: SparkContext, d: String): Dct = new Dct(sc, d)
}