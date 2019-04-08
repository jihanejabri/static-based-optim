package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query7(sc: SparkContext) {
  val sparqlQuery7 =
    """ PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?X ?Y WHERE {
                            <http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y
                            { ?X rdf:type ub:UndergraduateStudent }
                            UNION
                            { ?X rdf:type ub:GraduateStudent }
                            { ?Y rdf:type ub:Course }
                            UNION
                            { ?Y rdf:type ub:GraduateCourse }
                            ?X ub:takesCourse ?Y . }"""

  println("query: " + sparqlQuery7)

  val query = QueryFactory.create(sparqlQuery7)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q7 =" + (end - start) + "ms")
}
object Query7 {
  def apply(sc: SparkContext): Query7 = new Query7(sc)
}
