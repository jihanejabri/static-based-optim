package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query6(sc: SparkContext) {
  val sparqlQuery6 =
    """ PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?x WHERE {
                        {?x rdf:type ub:GraduateStudent . }
                        UNION
                        {?x rdf:type ub:UndergraduateStudent . }
                        }"""

  println("query: " + sparqlQuery6)

  val query = QueryFactory.create(sparqlQuery6)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis
  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q6 =" + (end - start))
}
object Query6 {
  def apply(sc: SparkContext): Query6 = new Query6(sc)
}

