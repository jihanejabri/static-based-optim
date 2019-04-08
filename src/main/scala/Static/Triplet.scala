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

  val nsUB = "http://www.univ-mlv.fr/~ocure/lubm.owl"
  val nsRDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  val nameSpaces = Map( "ub" -> nsUB, "rdf" -> nsRDF)
  def qname(ns: String, prop: String): String = nameSpaces.get(ns).get + "#" + prop


  def TripletGraphRequest(triple : List[Triple]){
    val newTriples  = List[Triple]()
    triple.foreach(f = triple => {
      // recupérer les triplets de la clause where
      val s = triple.getSubject()
      val o = triple.getObject()
      val p = triple.getPredicate()

      println("Triple : " + s + "," + p + "," + o)
      println("Subject : " + s)
      println("Predicate : " + p)
      println("Object : " + o)

      val typee = qname("rdf", "type")
      val GraduateStudentt = qname("ub", "GraduateStudent")
      val takesCoursee = qname("ub", "takesCourse")
      val w  = 0
      val typeCount : Double = 20659.0 // Statistic dejà calculé dans la classe Dct
      val GraduateStudentCount : Double = 1889.0
      val takesCourseeCount : Double = 21489.0
      val nbTriplets : Double = 103104.0 // nb total de triplets (déja calculé)
      if (p.toString() == typee) {
        val w1 : Double = typeCount / nbTriplets * 100
        println("/*/*/*/*/*/*/*/*/*/*/*/*/*/ Predicat type !!!!!!!!!!" + w1 + "%")
      }
      else if (p.toString() == takesCoursee) {
        val w2 : Double = takesCourseeCount / nbTriplets * 100
        println("/*/*/*/*/*/*/*/*/*/*/*/*/*/ Predicat takesCoursee !!!!!!!!!!" + w2 + "%")
      }
     // println("==========================" + takesCoursee)
    })

  }

  def calculWeight(): Unit ={
   // val a1 = s"predicate = $typee and object = $GraduateStudentt"
   // val a2 = s"predicate = $takesCoursee and object=$o2"

    // val w1 = trilpeDF.where(a1).select("subject")
    /*  val w2 = trilpeDF.where(a2).select("subject")
      val r1 = w1.join(w2)*/
    //  println("query1***********"+w1)
  }

}
