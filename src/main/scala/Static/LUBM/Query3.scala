package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query3(sc: SparkContext) {
  val sparqlQuery3 = """SELECT ?x WHERE {
                      ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#Publication> .
                      ?x <http://swat.cse.lehigh.edu/onto/univ-bench.owl#publicationAuthor> <http://www.Department0.University0.edu/AssistantProfessor0>
                      }"""

  println("query: " + sparqlQuery3)

  val query = QueryFactory.create(sparqlQuery3)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis
  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q3 =" + (end - start))
}
object Query3 {
  def apply(sc: SparkContext): Query3 = new Query3(sc)
}
