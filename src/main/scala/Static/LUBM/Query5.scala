package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query5(sc: SparkContext) {
  val sparqlQuery5 =
    """ PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                SELECT ?x  WHERE {
                            {?x rdf:type ub:AssistantProfessor . }
                            UNION
                            {?x rdf:type ub:AssociateProfessor . }
                            UNION
                            {?x rdf:type ub:FullProfessor . }
                            UNION
                            {?x rdf:type ub:GraduateStudent . }
                            UNION
                            {?x rdf:type ub:Lecturer . }
                            UNION
                            {?x rdf:type ub:UndergraduateStudent . }
                            {?x ub:memberOf <http://www.Department0.University0.edu> .}
                            UNION
                            {?x ub:worksFor <http://www.Department0.University0.edu> .}
                         }"""

  println("query: " + sparqlQuery5)

  val query = QueryFactory.create(sparqlQuery5)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis
  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q5 =" + (end - start) + "ms")
}
object Query5 {
  def apply(sc: SparkContext): Query5 = new Query5(sc)
}
