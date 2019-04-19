package Static.LUBM

import Static.Triplet
import org.apache.jena.graph.Triple
import org.apache.jena.query._
import org.apache.jena.sparql.algebra.{Algebra, OpVisitor}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class Query3(sc: SparkContext) {

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
  val sparqlQuery3 = """
                      PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                      PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>
                      SELECT ?x WHERE {
                      ?x rdf:type ub:Publication .
                      ?x ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0>
                      }"""

  println("query: " + sparqlQuery3)

  val query = QueryFactory.create(sparqlQuery3)

  val dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")
  val start = java.lang.System.currentTimeMillis
  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results : ResultSet = queryExec.execSelect()
  ResultSetFormatter.out(results)
  val end: Long = java.lang.System.currentTimeMillis
  println("Duration Q3 =" + (end - start) + "ms")

  val op = Algebra.compile(query)

  op.visit(new OpVisitor {
    override def visit(opBGP: OpBGP): Unit = {
      BGP(query, opBGP)
      println("oder triples :")
      OrderTriples(opBGP)
    }

    override def visit(quadPattern: OpQuadPattern): Unit = ???

    override def visit(quadBlock: OpQuadBlock): Unit = ???

    override def visit(opTriple: OpTriple): Unit = ???

    override def visit(opQuad: OpQuad): Unit = ???

    override def visit(opPath: OpPath): Unit = ???

    override def visit(opTable: OpTable): Unit = ???

    override def visit(opNull: OpNull): Unit = ???

    override def visit(opProc: OpProcedure): Unit = ???

    override def visit(opPropFunc: OpPropFunc): Unit = ???

    override def visit(opFilter: OpFilter): Unit = ???

    override def visit(opGraph: OpGraph): Unit = ???

    override def visit(opService: OpService): Unit = ???

    override def visit(dsNames: OpDatasetNames): Unit = ???

    override def visit(opLabel: OpLabel): Unit = ???

    override def visit(opAssign: OpAssign): Unit = ???

    override def visit(opExtend: OpExtend): Unit = ???

    override def visit(opJoin: OpJoin): Unit = ???

    override def visit(opLeftJoin: OpLeftJoin): Unit = ???

    override def visit(opUnion: OpUnion): Unit = ???

    override def visit(opDiff: OpDiff): Unit = ???

    override def visit(opMinus: OpMinus): Unit = ???

    override def visit(opCondition: OpConditional): Unit = ???

    override def visit(opSequence: OpSequence): Unit = ???

    override def visit(opDisjunction: OpDisjunction): Unit = ???

    override def visit(opExt: OpExt): Unit = ???

    override def visit(opList: OpList): Unit = ???

    override def visit(opOrder: OpOrder): Unit = ???

    override def visit(opProject: OpProject): Unit = {
      opProject.getSubOp.visit(this)
    }

    override def visit(opReduced: OpReduced): Unit = ???

    override def visit(opDistinct: OpDistinct): Unit = ???

    override def visit(opSlice: OpSlice): Unit = ???

    override def visit(opGroup: OpGroup): Unit = ???

    override def visit(opTop: OpTopN): Unit = ???
  })

  println("op:" + op)
  val op1 = Algebra.optimize(op)
  println("op:" + op1)

  val start1 = java.lang.System.currentTimeMillis
  val qIter : QueryIterator = Algebra.exec(op1, dataset)

  val results1 = 0
  while (qIter.hasNext()) {
    val b : Binding = qIter.nextBinding()
    results1 + 1
    System.out.println("b: "+b)
  }
  qIter.close()
  val end1: Long = java.lang.System.currentTimeMillis
  println("Duration Q3 =" + (end1 - start1) + "ms")
}
object Query3 {
  def apply(sc: SparkContext): Query3 = new Query3(sc)
}
