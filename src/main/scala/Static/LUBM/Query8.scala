package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query8(sc: SparkContext) {
  val sparqlQuery8 =
    """ PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?X ?Y ?Z WHERE {
                          { ?X rdf:type ub:UndergraduateStudent }
                          UNION
                          { ?X rdf:type ub:GraduateStudent }
                            ?Y rdf:type ub:Department .
                            ?X ub:memberOf ?Y .
                            ?Y ub:subOrganizationOf <http://www.University0.edu> .
                            ?X ub:emailAddress ?Z }"""

  println("query: " + sparqlQuery8)

  val query = QueryFactory.create(sparqlQuery8)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q8 =" + (end - start) + "ms")
}
object Query8 {
  def apply(sc: SparkContext): Query8 = new Query8(sc)
}
