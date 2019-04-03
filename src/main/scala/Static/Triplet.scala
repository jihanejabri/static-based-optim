package Static
import org.apache.jena.graph.Triple
object Triplet {
  def TripletGraphRequest(triple : List[Triple]){
    val newTriples  = List[Triple]()
    triple.foreach(triple =>{
      // recupérer les triplets de la clause where
      val s = triple.getSubject()
      val o = triple.getObject()
      val p = triple.getPredicate()

      println("Triple : " + s + "," + p + "," + o)
      println("Subject : " + s)
      println("Predicate : " + p)
      println("Object : " + o)
      println("==========================")

      // Pour ch chacun des triplets dans une clause wherer creer un noeud dans un gfaphe

    /*  val NodeS : Node  = triple.getSubject()
      if(NodeS.isVariable()){
        val name : String = NodeS.getLocalName()
         val sn = NodeFactory.createURI(name)
          NodeS.equals(sn)
      }
      val NodeP : Node  = triple.getPredicate()
      if(NodeP.isURI()){
        val name : String = NodeP.getLocalName()
        val sn = NodeFactory.createURI(name)
        NodeP.equals(sn)
      }
      val NodeO : Node  = triple.getObject()
      if(NodeO.isURI()){
        val name : String = NodeO.getLocalName()
        val sn = NodeFactory.createURI(name)
        NodeO.equals(sn)
      }
      //newTriples.add(Triple.create(NodeS,NodeP,NodeO))*/
    })
    /* val tripleList = List(s,p,o)
      println("Liste : "+ tripleList)*/
  }

}
