package Static
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.BasicPattern

import scala.collection.JavaConverters._
object Triplet {

  val nsUB = "http://swat.cse.lehigh.edu/onto/univ-bench.owl";
  val nsRDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  val nameSpaces = Map( "ub" -> nsUB, "rdf" -> nsRDF)
  val graduateCourse0 = "http://www.Department0.University0.edu/GraduateCourse0"

  // nb total de triplets (déja calculé dans la classe Dct)
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
  val graduateCourse0Count : Double = 7.0
  val publicationCount : Double = 6000
  val publicationAuthorCount :Double = 10634
  val AssistantProfessor0Count : Double = 918
  val ResearchGroupCount : Double = 224
  val University0EduCount  : Double = 34
  val UndergraduateStudentCount : Double = 5916
  val worksForCount : Double = 540
  def Triplename(ns: String, prop: String): String = nameSpaces.get(ns).get + "#" + prop

  val typee = Triplename("rdf", "type")
  val GraduateStudentt = Triplename("ub", "GraduateStudent")
  val takesCoursee = Triplename("ub", "takesCourse")
  val universityy = Triplename("ub", "University")
  val Departmentt = Triplename("ub", "Department")
  val Publication = Triplename("ub","Publication")
  val PublicationAuthor = Triplename("ub", "publicationAuthor")
  val memberOf = Triplename("ub", "memberOf")
  val subOrganizationOf = Triplename("ub", "subOrganizationOf")
  val emailAddress = Triplename("ub", "emailAddress")
  val ResearchGroup = Triplename("ub", "ResearchGroup")
  val worksFor = Triplename("ub", "worksFor")

  val JoinVars : List[String] = List()
  val lastJoinVars : List[String] = List()


  def TripletGraphRequests(triple : List[Triple]){
    triple.foreach(f = triple => {
      // recupérer les triplets de la clause where
      val s = triple.getSubject()
      val o = triple.getObject()
      val p = triple.getPredicate()

      println("Triple : " + s + "," + p + "," + o)
      println("Subject : " + s)
      println("Predicate : " + p)
      println("Object : " + o)

      println("Triple : "+getVarsOfTriple(triple))
    })
  }

   def getVarsOfTriple(t: Triple): List[String] = {
     val vars : List[String] = List()

     val subject = t.getSubject
     val predicate = t.getPredicate
     val obj = t.getObject

     val trilpeDF: Double = 103104.0 //dejà calculé dans la classe Dct
     val typeCount : Double = 20659.0
     if (subject.isVariable && predicate.isURI && obj.isURI) {
       var sel1: Double = 0.0
       var sel2: Double = 0.0
       var sel:Double =0.0
       if(predicate.toString() == typee || predicate.toString() == worksFor || predicate.toString() == subOrganizationOf  && obj.toString() == GraduateStudentt || obj.toString() == universityy || obj.toString == Departmentt  ||  obj.toString() == ResearchGroup){
         val select1 =  (typeCount/trilpeDF)
         val select2 =  (GraduateStudentCount/trilpeDF)
         val select3 =  (UniversityCount/trilpeDF)
         val select4 =  (DepartmentCount/trilpeDF)
         val select8 =  (subOrganizationOfCount/trilpeDF)
         val select9 =  (worksForCount/trilpeDF)
         val select10 =  (ResearchGroupCount/trilpeDF)

         println("///// Pred type " + select1 + "%")
         println("///// Concept GraduateStudent " + select2 + "%")
         println("///// Concept University " + select3 + "%")
         println("///// Concept Department " + select4 + "%")
         sel1 = 0.33*select1 + 0.33*select2
         sel1 = 0.33*select1 + 0.33*select3
         sel1 = 0.33*select1 + 0.33*select4
         sel1 = 0.33*select1 + 0.33*select8
         sel1 = 0.33*select1 + 0.33*select9
         sel1 = 0.33*select1 + 0.33*select10
         println("selctivity:"+sel1)
         println(vars :+ subject.getName + "," + predicate.getURI + "," + obj.getURI + "," + sel1)
       }
       if(obj.toString() == graduateCourse0 && predicate.toString() == takesCoursee) {
         val select5 =  (graduateCourse0Count/trilpeDF)
         val select6 =  (takesCourseeCount/trilpeDF)
         println("///// Obj graduateCourse " + select5 + "%")
         println("///// Concept takesCoursee " + select6 + "%")

         sel2 =  0.33*select5 + 0.33*select6
         println("selctivity:"+sel2)
         println(vars :+ subject.getName + "," + predicate.getURI + "," + obj.getURI + "," + sel2)
       }
       // choose min sel
       println("selctivity1Test:"+sel1)
       println("selctivity2Test:"+sel2)
       min(sel1, sel2)
       println(vars :+ subject.getName + "," + predicate.getURI + "," + obj.getURI)
     }
     vars
   }

    def min(s1:Double, s2:Double):Double = {
     if(s1<s2) return s1
     else return s2
    }

  def getSameVars(leftPart : List[String], rightPart : List[String]): List[String] ={
    val Vars : List[String] = List()
    var i : Int = 0

    while ({i < rightPart.size}) {
      if (leftPart.contains(rightPart.lift(i)))
        Vars:+(rightPart.lift(i))
      i += 1
    }
    Vars
  }

  var inputPattern: BasicPattern =  new BasicPattern()
   def hasSameVars(triplePosition: Int): Boolean = {
     val triple : Triple = inputPattern.get(triplePosition)

     val tripleVars : List[String] = getVarsOfTriple(triple)
     var i = 0
     while ({i < inputPattern.size}) {
       if ((i != triplePosition) &&  getSameVars(getVarsOfTriple(inputPattern.get(i)), tripleVars)==true) return true
       i += 1
     }
    return false
  }

  def chooseFirstTriple(): Int = { // choose the first triple with min sel
    var i = 0
    while ( {
      i < inputPattern.size
    }) {
      if (hasSameVars(i)) return i
      i += 1
    }
    return 0
  }

  def chooseNextTriple(): Int ={
    var nextTriple = 0
    var numOfTVars = 0
    var i = 0

    while ({ i < inputPattern.size }) {
      val tripleVars = getVarsOfTriple(inputPattern.get(i))
      val sharedVars = getSameVars(JoinVars, tripleVars)
      if (tripleVars.size > numOfTVars) {
        nextTriple = i
        numOfTVars = tripleVars.size
      }
        i += 1
    }
    return nextTriple    // return the first pattern
  }


  def reorderTriples(pattern: BasicPattern): BasicPattern ={
    val outputPattern : BasicPattern = new BasicPattern()
    inputPattern = pattern
    val triples: List[Triple] = inputPattern.asScala.toList
    var idx: Int = chooseFirstTriple()
    var triple: Triple = triples(idx)
    outputPattern.add(triple)
    getVarsOfTriple(triple)
    triples.drop(idx)

    if(idx<triples.size) {
      idx = chooseNextTriple()
      outputPattern.add(triple)
      getVarsOfTriple(triple)
      triples.drop(idx)
    }
    outputPattern
  }
}
