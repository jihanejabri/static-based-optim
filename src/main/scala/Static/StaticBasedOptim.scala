package Static

import org.apache.jena.graph.{NodeFactory, Triple}

object StaticBasedOptim {
  def print(): Unit = {
    val rdfs = "http://www.w3.org/2000/01/rdf-schema";
    val ub = "http://www.univ-mlv.fr/~ocure/lubm.owl";
    val owl = "http://www.w3.org/2002/07/owl";
    val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns";

    val t1:Triple = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createLiteral(rdf, "type", true), NodeFactory.createLiteral(ub, "GraduateStudent"))

    val s1 = t1.getSubject
    val o1 = t1.getObject
    val p1 = t1.getPredicate

    println("Triple 1 : " + s1 + " " + p1 + " " + o1)

    val t2:Triple = Triple.create(NodeFactory.createVariable("x"), NodeFactory.createLiteral(ub, "takesCourse", true), NodeFactory.createURI("<http://www.Department0.University0.edu/GraduateCourse0>"))
    val s2 = t2.getSubject
    val o2 = t2.getObject
    val p2 = t2.getPredicate

    println("Triple 2 : " + s2 + " " + p2 + " " + o2)
  }
}
