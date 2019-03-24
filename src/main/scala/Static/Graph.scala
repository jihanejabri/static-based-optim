package Static

import org.apache.jena.graph.Triple

object Graph {
  def TripleGraph(triple : List[Triple])={
    triple.foreach(triple =>{
      val o = triple.getObject
      val p = triple.getPredicate
      val s = triple.getSubject

      if(o.isVariable && p.isVariable && s.isVariable) {
        print("Triple : " + o + "," + "p" + "," + "s")
      }
    })
  }
}
