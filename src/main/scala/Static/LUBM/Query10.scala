package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query10(sc: SparkContext) {
  val sparqlQuery10 ="""
                       PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                       PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       SELECT ?x WHERE {
                            ?x rdf:type ub:GraduateStudent .
                            ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>}"""

  println("query: " + sparqlQuery10)

  val query = QueryFactory.create(sparqlQuery10)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q10 =" + (end - start) + "ms")
}
object Query10 {
  def apply(sc: SparkContext): Query10 = new Query10(sc)
}
