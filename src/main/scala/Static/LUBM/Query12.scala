package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query12(sc: SparkContext) {
  val sparqlQuery12 ="""
                       PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                       PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       SELECT ?X ?Y WHERE {
                                      ?X rdf:type [] .
                                      ?Y rdf:type ub:Department .
                                      ?X ub:worksFor ?Y .
                                      ?x ub:subOrganizationOf <http://www.University0.edu> }"""

  println("query: " + sparqlQuery12)

  val query = QueryFactory.create(sparqlQuery12)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q12 =" + (end - start))
}
object Query12 {
  def apply(sc: SparkContext): Query12 = new Query12(sc)
}

