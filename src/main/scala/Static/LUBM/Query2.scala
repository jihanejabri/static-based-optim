package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query2(sc: SparkContext) {
  val sparqlQuery2 =
                    """PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
                       PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                       PREFIX owl:<http://www.w3.org/2002/07/owl#>
                       PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       SELECT ?X ?Y ?Z WHERE {
                              ?X rdf:type ub:GraduateStudent .
                              ?Y rdf:type ub:University .
                              ?Z rdf:type ub:Department .
                              ?X ub:memberOf ?Z .
                              ?Z ub:subOrganizationOf ?Y .
                              ?X ub:undergraduateDegreeFrom ?Y}"""

  println("query: " + sparqlQuery2)

  val query = QueryFactory.create(sparqlQuery2)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q2 =" + (end - start) + "ms")
}
object Query2 {
  def apply(sc: SparkContext): Query2 = new Query2(sc)
}
