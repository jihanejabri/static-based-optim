package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query5(sc: SparkContext) {
  val sparqlQuery5 =
    """ SELECT ?x  WHERE {
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
                            {?x <http://swat.cse.lehigh.edu/onto/univ-bench.owl#memberOf> <http://www.Department0.University0.edu> .}
                            UNION
                            {?x <http://swat.cse.lehigh.edu/onto/univ-bench.owl#worksFor> <http://www.Department0.University0.edu> .}
                         }"""

  println("query: " + sparqlQuery5)

  val query = QueryFactory.create(sparqlQuery5)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
}
object Query5 {
  def apply(sc: SparkContext): Query5 = new Query5(sc)
}
