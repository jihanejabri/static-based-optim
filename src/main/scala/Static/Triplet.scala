package Static
import org.apache.jena.graph.Triple

object Triplet {
  def TripletGraphRequest(triple : List[Triple]){
    triple.foreach(triple =>{
      // Pour ch chacun des triplets dans une clause wherer creer un noeud dans un gfaphe
      val s = triple.getSubject
      val o = triple.getObject
      val p = triple.getPredicate

      println("Triple : " + s + "," + p + "," + o)
      println("Subject : " + s)
      println("Predicate : " + p)
      println("Object : " + o)
      println("==========================")
    })

    //essayer de stocker chaque triplet dans un tab et représenter le poid de chaque composant et ensuite executer selon l'ordre

  }
}
