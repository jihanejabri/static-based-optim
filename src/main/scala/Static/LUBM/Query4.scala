package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query4 (sc: SparkContext) {
  val sparqlQuery4 =
                    """ SELECT ?x ?y1 ?y2 ?y3 WHERE {
                            {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#AssociateProfessor> . }
                            UNION
                            {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#AssistantProfessor> . }
                            UNION
                            {?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#FullProfessor> . }
                            ?x <http://swat.cse.lehigh.edu/onto/univ-bench.owl#worksFor> <http://www.Department0.University0.edu> .
                            ?x <http://swat.cse.lehigh.edu/onto/univ-bench.owl#name> ?y1 .
                            ?x <http://swat.cse.lehigh.edu/onto/univ-bench.owl#emailAddress> ?y2 .
                            ?x <http://swat.cse.lehigh.edu/onto/univ-bench.owl#telephone> ?y3
                            }"""

  println("query: " + sparqlQuery4)

  val query = QueryFactory.create(sparqlQuery4)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
}
object Query4 {
  def apply(sc: SparkContext): Query4 = new Query4(sc)
}
