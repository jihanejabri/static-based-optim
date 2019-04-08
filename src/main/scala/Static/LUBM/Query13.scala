package Static.LUBM

import org.apache.jena.query._
import org.apache.spark.SparkContext

class Query13(sc: SparkContext) {
  val sparqlQuery13 ="""
                       PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                       PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                       SELECT ?X WHERE {
                               { ?X rdf:type ub:Lecturer  . } UNION
                               { ?X rdf:type ub:AssistantProfessor  . } UNION
                               { ?X rdf:type ub:AssociateProfessor  . } UNION
                               { ?X rdf:type ub:FullProfessor  . } UNION
                               { ?X rdf:type ub:TeachingAssistant  . } UNION
                               { ?X rdf:type ub:ResearchAssistant  . } UNION
                               { ?X rdf:type ub:GraduateStudent  . } UNION
                               { ?X rdf:type ub:UndergraduateStudent  . } UNION
                               { ?X ub:undergraduateDegreeFrom <http://www.University0.edu> . } UNION
                               { ?X ub:mastersDegreeFrom <http://www.University0.edu> . } UNION
                               { ?X ub:doctoralDegreeFrom <http://www.University0.edu> }}"""

  println("query: " + sparqlQuery13)

  val query = QueryFactory.create(sparqlQuery13)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q13 =" + (end - start) + "ms")
}
object Query13 {
  def apply(sc: SparkContext): Query13 = new Query13(sc)
}
