package Static
import org.apache.jena.graph.Triple

object Triplet {
  def TripletGraphRequest(triple : List[Triple])={
    triple.foreach(triple =>{
      // Pour ch chacun des triplets dans une clause wherer creer un noeud dans un gfaphe
      val s = triple.getSubject
      val o = triple.getObject
      val p = triple.getPredicate
      
      if(s.isVariable && p.isVariable && o.isVariable) {
        print("Triple : " + s + "," + p + "," + o)
      }
    })

    //essayer de stocker chaque triplet dans un tab et représenter le poid de chaque composant et ensuite executer selon l'ordre

  }

  /*def nodeToId(n: Node): Option[Long] = {
    n match {
    }
  }
*/
 /* def toTriplePattern(basicPattern: BasicPattern): Option[Array[Long]] = {
    val triples = {
      basicPattern.getList match {
        case al: ArrayList[Triple] => al
        case other => new ArrayList[Triple](other)
      }
    }
    val patterns = new Array[Long](triples.size)
    var i = 0
    while (i < triples.size) {
      val triple = triples(i)
      val patternIndex = i
     /* val sId = nodeToId(triple.getSubject).getOrElse(return None)
      val pId = nodeToId(triple.getPredicate).getOrElse(return None)
      val oId = nodeToId(triple.getObject).getOrElse(return None)
      patterns(patternIndex + cardinalityOffset) = Long.MaxValue
      patterns(patternIndex + subjectOffset) = sId
      patterns(patternIndex + predicateOffset) = pId
      patterns(patternIndex + objectOffset) = oId*/
      i += 1
    }
    Some(patterns)
  }
*/
  /*def bestRequest(triples: List[Triple])={
      var id = 0;
      val verticle = triples.mapConserve(triple =>{
        val o = triple.getObject
        val p = triple.getPredicate
        val s = triple.getSubject
        /*** On créer les sommets du graph ***/
        id+=1
        if (o.isVariable && p.isVariable && s.isVariable) {
          (id.toLong,(s,p,o))
        }

      })
    /*** créer les aretes du graph***/
  }

  def create(query: Query, sparql: Sparql): GraphulaExecution = {
    val transaction = sparql.transaction
    query.setResultVars()
    val context = ARQ.getContext
    val dataset = DatasetFactory.create(sparql.model)
    val dsg = dataset.asDatasetGraph()
  }*/
}
