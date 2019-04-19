package Static.LUBM

import Static.Triplet
import org.apache.jena.graph.Triple
import org.apache.jena.query._
import org.apache.jena.sparql.algebra.op._
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class Query4 (sc: SparkContext) {

  def BGP(line : Query, opBGP: OpBGP): Unit = {
    val res = opBGP.getPattern.asScala
    // println("Triples :--"+Triplet.TripletGraphRequests(OrderAndGetBGPs(res.toList)))
    val triples  = Triplet.TripletGraphRequests(res.toList)
    println("Triples :--"+triples)
  }

  def OrderAndGetBGPs(bgps: List[Triple]): List[Triple] ={
    return bgps
  }

  def OrderTriples(opBGP: OpBGP): Unit ={
    val t1 = Triplet.reorderTriples(opBGP.getPattern())
    println("t1:"+t1)
  }
  val sparqlQuery4 =
                    """ PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                        SELECT ?x ?y1 ?y2 ?y3 WHERE {
                            {?x rdf:type ub:AssociateProfessor . }
                            UNION
                            {?x rdf:type ub:AssistantProfessor . }
                            UNION
                            {?x rdf:type ub:FullProfessor . }
                            ?x ub:worksFor <http://www.Department0.University0.edu> .
                            ?x ub:name ?y1 .
                            ?x ub:emailAddress ?y2 .
                            ?x ub:telephone ?y3
                            }"""

  println("query: " + sparqlQuery4)

  val query = QueryFactory.create(sparqlQuery4)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q4 =" + (end - start) + "ms")

}
object Query4 {
  def apply(sc: SparkContext): Query4 = new Query4(sc)
}
