package Static

import org.apache.jena.graph.Triple

object Graph {
  def TripleGraph(triple : List[Triple])={
    triple.foreach(triple =>{
      val s = triple.getSubject
      val o = triple.getObject
      val p = triple.getPredicate


      if(s.isVariable && p.isVariable && o.isVariable) {
        print("Triple : " + s + "," + "p" + "," + "o")
      }
    })
  }
}
