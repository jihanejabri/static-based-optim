package Static
import org.apache.jena.graph.Triple
object Triplet {

  /***************************** Statics : nb Concepts*********/
  val Work : Long = 13312
  val Faculty : Long = 13184
  val Person : Long = 12288
  val Student : Long = 12800
  val GraduateStudent : Long = 12864
  val Chair : Long = 13201
  val Professor : Long  = 13200
  val FullProfessor : Long  = 13205
  val Department : Long  = 10496
  val Course : Long  = 13568
  val Publication : Long  = 11264
  val UndergraduateStudent : Long  = 12928
  val AssociateProfessor : Long  = 13203
  val ResearchGroup : Long  = 10880
  val GraduateCourse : Long = 13696
  val Organization : Long  = 10240
  val AssistantProfessor : Long  = 13206
  val Lecturer : Long  = 13208
  val University : Long  = 11008

  /***************************** Statics : nb Properties********/
  val telephone : Long = 672
  val researchProject : Long = 772
  val advisor : Long = 848
  val headOf : Long = 803
  val undergraduateDegreeFrom : Long = 785
  val member : Long = 788
  val emailAddress : Long = 736
  val _type : Long = 0
  val subOrganizationOf : Long = 840
  val publicationAuthor : Long = 812
  val takesCourse : Long = 792
  val hasAlumnus : Long = 776
  val worksFor : Long = 802
  val memberOf : Long = 800
  val teacherOf : Long = 828
  val name : Long = 576
  val degreeFrom : Long = 784
  val ub = "http://www.univ-mlv.fr/~ocure/lubm.owl";
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns";

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
