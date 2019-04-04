package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query14(sc: SparkContext) {
  val sparqlQuery14 ="""
                       PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                       PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       SELECT ?X WHERE {
                                        ?X rdf:type ub:UndergraduateStudent }"""

  println("query: " + sparqlQuery14)

  val query = QueryFactory.create(sparqlQuery14)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
}
object Query14 {
  def apply(sc: SparkContext): Query14 = new Query14(sc)
}

