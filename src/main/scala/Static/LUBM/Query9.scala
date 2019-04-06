package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query9(sc: SparkContext) {
  val sparqlQuery9 =
    """ PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
        PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?X ?Y ?Z WHERE {
                          { ?X rdf:type ub:UndergraduateStudent }
                          UNION
                          { ?X rdf:type ub:GraduateStudent }
                            ?X ub:advisor ?Y .
                          { ?Y rdf:type ub:AssistantProfessor . }
                          UNION
                          { ?Y rdf:type ub:AssociateProfessor . }
                          UNION
                          { ?Y rdf:type ub:FullProfessor . }
                            ?Y ub:teacherOf ?Z .
                          { ?Z rdf:type ub:Course }
                          UNION
                          { ?Z rdf:type ub:GraduateCourse }
                          ?X ub:takesCourse ?Z
                           }"""

  println("query: " + sparqlQuery9)

  val query = QueryFactory.create(sparqlQuery9)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis


  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q9 =" + (end - start))
}
object Query9 {
  def apply(sc: SparkContext): Query9 = new Query9(sc)
}

