package Static

import org.apache.jena.graph.{Node, Triple}

object StaticBasedOptim {
  def print(): Unit = {

    var t1: Triple = Triple.create(new Node.NodeMaker {}," "," ")

   val t: List[Triple] = List("?x rdf:type ub:GraduateStudent","")

  //  val triple = t.toList
    println("triple:"+t)
    Triplet.TripletGraphRequest(t)
    println("Triplet :"+Triplet.TripletGraphRequest(t))
    println("--------------------------------------------")*/
  }
/*
  def getT(elementPathBlock: ElementPathBlock){
    val triples = Iterator<TriplePath>
    triples = elementPathBlock.patternElts()
    val newTriples = List<Triple>
    newTriples = new ArrayList<>()
    while(triples.hasNext()){
      val triple = TriplePath  = triples.next();
      if(triple.isTriple()) {
        Node s = triple.getSubject();
        if(s.isURI()) {
          String localName = s.getLocalName();
          s = NodeFactory.createURI(ns + localName);
        }
        Node p = triple.getPredicate();
        if(p.isURI()) {
          String localName = p.getLocalName();
          p = NodeFactory.createURI(ns + localName);
        }
        Node o = triple.getSubject();
        if(o.isURI()) {
          String localName = o.getLocalName();
          o = NodeFactory.createURI(ns + localName);
        }

        newTriples.add(Triple.create(s, p, o));


      } else {
        // TODO handle triple path
      }
      triples.remove();
    }
    for(Triple t : newTriples) {
      el.addTriple(t);
    }
  }*/
}
