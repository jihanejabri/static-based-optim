package Static
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.BasicPattern
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

  val nsUB = "http://swat.cse.lehigh.edu/onto/univ-bench.owl";
  val nsRDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  val nameSpaces = Map( "ub" -> nsUB, "rdf" -> nsRDF)
  def qname(ns: String, prop: String): String = nameSpaces.get(ns).get + "#" + prop

  def TripletGraphRequest(triple : List[Triple]){
    //val newTriples  = List[String]()
    triple.foreach(f = triple => {
      // recupérer les triplets de la clause where
      val s = triple.getSubject()
      val o = triple.getObject()
      val p = triple.getPredicate()

      println("Triple : " + s + "," + p + "," + o)
      println("Subject : " + s)
      println("Predicate : " + p)
      println("Object : " + o)

      //val newTriples  = getVarsOfTriple(triple)
      println("Triple : "+getVarsOfTriple(triple))

      val typee = qname("rdf", "type")
      val GraduateStudentt = qname("ub", "GraduateStudent")
      val takesCoursee = qname("ub", "takesCourse")

      val w  = 0

      // nb total de triplets (déja calculé dans sla classe Dct)
      val nbTriplets : Double = 103104.0

      // Statistic dejà calculé dans la classe Dct
      val typeCount : Double = 20659.0
      val GraduateStudentCount : Double = 1889.0
      val takesCourseeCount : Double = 21489.0
      val UniversityCount : Double = 3510.0
      val DepartmentCount : Double = 15.0
      val memberOfCount : Double = 7790.0
      val subOrganizationOfCount : Double = 239.0
      val undergraduateDegreeFromCount : Double = 2414.0

      if (p.toString() == typee) {//20.03%
      val w1 : Double = typeCount / nbTriplets * 100
        println("/*/*/*/*/*/*/*/*/*/*/*/*/*/ Predicat type !!!!!!!!!!" + w1 + "%")
      }
      if (o.toString() == GraduateStudentt) {//1.8%
        val w2 : Double = GraduateStudentCount / nbTriplets * 100
        println("/*/*/*/*/*/*/*/*/*/*/*/*/*/ concept GraduateStudent !!!!!!!!!!" + w2 + "%")
      }
      if (p.toString() == takesCoursee) {//20.84%
      val w3 : Double = takesCourseeCount / nbTriplets * 100
        println("/*/*/*/*/*/*/*/*/*/*/*/*/*/ Predicat takesCourse !!!!!!!!!!" + w3 + "%")
      }

    })

  }

   def getVarsOfTriple(t: Triple): List[String] = {
     val vars : List[String] = List()
     val typee = qname("rdf", "type")
     val GraduateStudentt = qname("ub", "GraduateStudent")
     val subject = t.getSubject
     val predicate = t.getPredicate
     val obj = t.getObject
     val sel : Double = 1.0
     val trilpeDF: Double = 103104.0 //dejà calculé dans la classe Dct
     val typeCount : Double = 20659.0
     if (subject.isVariable && predicate.isURI && obj.isURI) {
       if(predicate.toString() == typee){
         val select = sel * (typeCount/trilpeDF)
         println("///// Pred type " + select + "%")
         println(vars :+ subject.getName + "," + predicate.getURI + "," + obj.getURI + "," + select)
       }
       println(vars :+ subject.getName + "," + predicate.getURI + "," + obj.getURI + "," + sel)

     }
    /* if (predicate.isURI)
       println(vars:+predicate.getURI)
     if (obj.isURI)
       println(vars:+obj.getURI)*/
     println("t:" + t)
     println("Var:" + vars)
     vars
   }

  def getSharedVars(leftSchema : List[String], rightSchema : List[String]): List[String] ={
    val sharedVars : List[String] = List()
    var i : Int = 0

    while ( {
      i < rightSchema.size
    }) {
      if (leftSchema.contains(rightSchema.lift(i)))
        sharedVars:+(rightSchema.lift(i))
      i += 1
    }
    sharedVars
  }

   val inputPattern : BasicPattern = new BasicPattern()
   def hasSharedVars(triplePos: Int): Boolean = {
     val triple : Triple = inputPattern.get(triplePos)
     val tripleVars : List[String] = getVarsOfTriple(triple)
     var i = 0
     while ( {
       i < inputPattern.size
     }) {
       if ((i != triplePos) && getSharedVars(getVarsOfTriple(inputPattern.get(i)), tripleVars).size > 0) return true
       i += 1
     }
    return false
  }

  def chooseFirst(): Int = {
    var i = 0
    while ( {
      i < inputPattern.size
    }) {
      if (hasSharedVars(i)) return i
      i += 1
    }
    return 0
  }

  def chooseNext(): Int ={
    return 0
  }

  def reorder(): Unit ={

  }

 /* val allJoinVars : List[String]
  def addToJoinVars(tripleVars : List[String]): Unit ={
    var field = String
    var i = 0
    while ( {
      i < tripleVars.size
    }) {
      field = tripleVars.get(i)
      if (!allJoinVars.contains(field)) allJoinVars:+(field)
        i += 1
    }
  }*/
}
