package Static
import org.apache.jena.query.{QueryFactory, _}
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.algebra.{Algebra, OpVisitor}
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

class Query1(sc: SparkContext) {

  def BGP(line : Query, opBGP: OpBGP): Unit = {
    val scalaIterable = opBGP.getPattern.asScala
    println("Triples :--"+Triplet.TripletGraphRequest(scalaIterable.toList))
  }


  val sparqlQuery1 = """
                                 PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
                                 PREFIX ub:<http://www.univ-mlv.fr/~ocure/lubm.owl#>
                                 PREFIX owl:<http://www.w3.org/2002/07/owl#>
                                 PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                 SELECT ?x WHERE {
                                   ?x rdf:type ub:GraduateStudent .
                                   ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>.}"""

  println("query: " + sparqlQuery1)

  val query = QueryFactory.create(sparqlQuery1)

  val dataset : Dataset = DatasetFactory.create("data/LUBMInstances/lubm1.ttl")

  val queryExec: QueryExecution = QueryExecutionFactory.create(query, dataset)
  val results = queryExec.execSelect()
  val op = Algebra.compile(query)

  op.visit(new OpVisitor {
    override def visit(opBGP: OpBGP): Unit = {
      BGP(query, opBGP)
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

  println("rdf :" +query.getPrefix("rdf"))
 /* while (results.hasNext()) {
    println("res:" +results.next())
  }
  println("Running as a query" + results.hasNext())
*/
/*var queryExec: QueryExecution = QueryExecutionFactory.create(query)
  val results = queryExec.execSelect()
*/
}

object Query1 {
def apply(sc: SparkContext): Query1 = new Query1(sc)
}