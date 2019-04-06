package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query11(sc: SparkContext) {
  val sparqlQuery11 ="""
                       PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                       PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       SELECT ?x WHERE {
                            rdf:type ub:ResearchGroup ?x .
                            ?x ub:subOrganizationOf <http://www.University0.edu> }"""

  println("query: " + sparqlQuery11)

  val query = QueryFactory.create(sparqlQuery11)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q11 =" + (end - start))
}
object Query11 {
  def apply(sc: SparkContext): Query11 = new Query11(sc)
}
