package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query13(sc: SparkContext) {
  val sparqlQuery13 ="""
                       PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                       PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       SELECT ?x WHERE {
                          {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#AssistantProfessor> . }
                           UNION
                          {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#AssociateProfessor> . }
                           UNION
                          {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#FullProfessor> . }
                           UNION
                          {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#GraduateStudent> . }
                           UNION
                          {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#Lecturer> . }
                           UNION
                          {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> . }
                          <http://www.University0.edu> ub:hasAlumnus ?x }"""

  println("query: " + sparqlQuery13)

  val query = QueryFactory.create(sparqlQuery13)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
}
object Query13 {
  def apply(sc: SparkContext): Query13 = new Query13(sc)
}
