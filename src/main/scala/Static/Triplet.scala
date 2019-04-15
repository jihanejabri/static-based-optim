package Static
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.core.BasicPattern
object Triplet {

  val nsUB = "http://swat.cse.lehigh.edu/onto/univ-bench.owl";
  val nsRDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns"
  val nameSpaces = Map( "ub" -> nsUB, "rdf" -> nsRDF)
  val graduateCourse0 = "http://www.Department0.University0.edu/GraduateCourse0"
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
  val graduateCourse0Count : Double = 7.0
  def qname(ns: String, prop: String): String = nameSpaces.get(ns).get + "#" + prop
  val typee = qname("rdf", "type")
  val GraduateStudentt = qname("ub", "GraduateStudent")
  val takesCoursee = qname("ub", "takesCourse")
  val universityy = qname("ub", "University")
  val Departmentt = qname("ub", "Department")

  def TripletGraphRequests(triple : List[Triple]){
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
      println("Triple : "+getVarsOfTriple1(triple))
     // println("test hassharedVarss:"+hasSharedVars(1))
    })
  }

   def getVarsOfTriple1(t: Triple): List[String] = {
     val vars : List[String] = List()
     val typee = qname("rdf", "type")
     val GraduateStudentt = qname("ub", "GraduateStudent")
     val subject = t.getSubject
     val predicate = t.getPredicate
     val obj = t.getObject

     val trilpeDF: Double = 103104.0 //dejà calculé dans la classe Dct
     val typeCount : Double = 20659.0
     if (subject.isVariable && predicate.isURI && obj.isURI) {
       var sel1: Double = 0.0
       var sel2: Double = 0.0
       var sel:Double =0.0
       if(predicate.toString() == typee && obj.toString() == GraduateStudentt || obj.toString() == universityy || obj.toString == Departmentt){
         val select1 =  (typeCount/trilpeDF)
         val select2 =  (GraduateStudentCount/trilpeDF)
         val select3 =  (UniversityCount/trilpeDF)
         val select4 =  (DepartmentCount/trilpeDF)
         println("///// Pred type " + select1 + "%")
         println("///// Concept GraduateStudent " + select2 + "%")
         println("///// Concept University " + select3 + "%")
         println("///// Concept Department " + select4 + "%")
         sel1 = 0.33*select1 + 0.33*select2
         sel1 = 0.33*select1 + 0.33*select3
         sel1 = 0.33*select1 + 0.33*select4

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
       println("selctivityTest:"+min(12.3,10.6))
       min(sel1, sel2)
       println("min sel:"+min(sel1, sel2))
       println(vars :+ subject.getName + "," + predicate.getURI + "," + obj.getURI + "," + min(sel1, sel2))
     }
     vars
   }

    def min(s1:Double, s2:Double):Double = {
     if(s1<s2) return s1
     else return s2
    }

  def getSharedVars(leftSchema : List[String], rightSchema : List[String]): List[String] ={
    val sharedVars : List[String] = List()
    var i : Int = 0

    while ({i < rightSchema.size}) {
      if (leftSchema.contains(rightSchema.lift(i)))
        sharedVars:+(rightSchema.lift(i))
      i += 1
    }
    sharedVars
  }

  var inputPattern: BasicPattern =  new BasicPattern()
   def hasSharedVars(triplePos: Int): Boolean = {
     val triple : Triple = inputPattern.get(triplePos)

     val tripleVars : List[String] = getVarsOfTriple1(triple)
     var i = 0
     while ({i < inputPattern.size}) {
       if ((i != triplePos) &&  getSharedVars(getVarsOfTriple1(inputPattern.get(i)), tripleVars)==true) return true
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
  val allJoinVars : List[String] = List()
  val lastJoinVars : List[String] = List()

  def chooseNext(): Int ={
    var nextTriple = 0
    var numOfJoinVars = 0
    var i = 0
    while ({ i < inputPattern.size }) {
      val tripleVars: List[String] = getVarsOfTriple1(inputPattern.get(i))
      val sharedVars: List[String] = getSharedVars(allJoinVars, tripleVars)
      if (sharedVars.size > numOfJoinVars) {
       // lastJoinVars.addAll(sharedVars)
        nextTriple = i
        numOfJoinVars = sharedVars.size
      }
        i += 1
    }
    // return the first pattern
    return nextTriple
  }
/*
  def reorder(pattern: BasicPattern): BasicPattern ={
    val outputPattern : BasicPattern = new BasicPattern()
    inputPattern = pattern
    val triples: List[Triple] = {
      inputPattern.asScala.toList
    }
    var idx: Int = chooseFirst
    var triple: Triple = triples.get(idx)
    outputPattern.add(triple)
    getVarsOfTriple1(triple)
    triples.drop(idx)

    while ({!(triples.isEmpty)}) {
      idx = chooseNext
      triple = triples.add(idx)
      outputPattern.add(triple)
      getVarsOfTriple1(triple)
      triples.drop(idx)
    }
    outputPattern
  }*/
}
