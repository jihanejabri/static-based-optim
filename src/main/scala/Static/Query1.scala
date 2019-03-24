package Static
import org.apache.jena.query.{QueryExecution, QueryExecutionFactory, QueryFactory}
import org.apache.spark.SparkContext

class Query1(sc: SparkContext) {
/*
 private var sparqlQuery1 = """
                               PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
                               PREFIX ub:<http://www.univ-mlv.fr/~ocure/lubm.owl#>
                               PREFIX owl:<http://www.w3.org/2002/07/owl#>
                               PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                               SELECT ?x WHERE {
                                 ?x rdf:type ub:GraduateStudent .
                                 ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>.}"""

  val query = QueryFactory.create(sparqlQuery1)

  val results = queryExec.execSelect
  var queryExec: QueryExecution = QueryExecutionFactory.create(query, /**model ou dataset**/)
*/
}

object Query1 {
  def apply(sc: SparkContext): Query1 = new Query1(sc)
}