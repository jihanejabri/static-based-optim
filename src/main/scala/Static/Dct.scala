package Static

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.DataFrame

class Dct(sc : SparkContext, dir : String) extends  Serializable {
  var lubmEncoded = "/LUBMInstances/lubm1Encoded.txt/"
  var lubmConcept = "/univ-bench_conceptsURL2Id.txt"
  var lubmProperties = "/univ-bench_propertiesURL2Id.txt"
  var propertiesId2Range = "/univ-bench_propertiesId2Range.txt"
  var propertiesId2Domain = "/univ-bench_propertiesId2Domain.txt"

  //  val data = sc.textFile(dir +lubmEncoded).map(x=>x.split(" "))
  val lubmEncodedData = sc.textFile(dir +lubmEncoded)
  println("PRINTING DATA ----- = "+lubmEncodedData.take(14).foreach(println))
  //  println(data)
  val conceptId2URI = lubmEncodedData.map(x=>x.split(" ")).map(x=>(x(0).toLong, x(1).toLong, x(2).toLong))
  //  conceptId2URI.persist()
  println("Data conceptId2URI count = "+conceptId2URI.count)
  println("conceptId2URI println= "+conceptId2URI.take(44).foreach(println))

  val lubmConceptData = sc.textFile(dir + lubmConcept)
  val lubmConceptData_ = lubmConceptData.map(x=>x.split("\t")).map(x=>(x(0), x(1),x(2), x(3)))
  //  conceptId2URII.persist()
  println("lubmConceptData_ count :"+lubmConceptData_.count)

  println("lubmConceptData_ println = "+lubmConceptData_.take(44).foreach(println))


  val lubmPropertiesData = sc.textFile(dir +lubmProperties)
  println("PRINTING DATA ----- = "+lubmPropertiesData.take(4).foreach(println))
  //  println(data)
  //  val lubmPropertiesData_ = lubmPropertiesData.map(x=>x.split("\t")).map(x=>(x(0), x(1), x(2), x(3), x(4), x(5)))
  val lubmPropertiesData_ = lubmPropertiesData.map(x=>x.split("\t")).map(x=>(x(0), x(1), x(2), x(3)))
  //  conceptId2URI.persist()
  println("Data conceptId2URI count = "+lubmPropertiesData_.count)
  println("conceptId2URI println= "+lubmPropertiesData_.take(4).foreach(println))

  val bench_propertiesId2RangeData = sc.textFile(dir + propertiesId2Range)
  println("PRINTING DATA ----- = "+bench_propertiesId2RangeData.take(4).foreach(println))

  val bench_propertiesId2RangeData_ = bench_propertiesId2RangeData.map(x=>x.split("\t")).map(x=>(x(0), x(1)))

  println("Data propertiesId2Range count = "+bench_propertiesId2RangeData_.count)
  println("propertiesId2Range println= "+bench_propertiesId2RangeData_.take(4).foreach(println))

  val bench_propertiesId2DomainData = sc.textFile(dir + propertiesId2Domain)
  println("PRINTING DATA ----- = "+bench_propertiesId2DomainData.take(4).foreach(println))

  val bench_propertiesId2DomainData_ = bench_propertiesId2DomainData.map(x=>x.split("\t")).map(x=>(x(0), x(1)))

  println("Data propertiesId2Domain count = "+bench_propertiesId2DomainData_.count)
  println("propertiesId2Domain println= "+bench_propertiesId2DomainData_.take(4).foreach(println))

  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val trilpesDF = conceptId2URI.toDF("subject", "predicat", "object")
  println("Triples = "+trilpesDF.count)
  println("trilpesDF println= "+trilpesDF.take(4).foreach(println))

  /***************************** Statics : nb Concepts*********/
 /* val model = ModelFactory.createDefaultModel()
  def numberOfConcepts()= {
    val rdfType = model.createProperty("1")
    val obj = model.createResource(trilpesDF)
    val iterator = model.listSubjectsWithProperty(rdfType,obj)
    iterator.toList.size()
  }*/
  val Work : Long = 13312
  val Faculty : Long = 13184
  val Person : Long = 12288
  val Institute : Long = 10624
  val Student : Long = 12800
  val GraduateStudent : Long = 12864
  val Employee : Long = 13056
  val VisitingProfessor : Long = 13202
  val Chair : Long = 13201
  val Thing : Long = 8192
  val AdministrativeStaff : Long = 13152
  val Book : Long  = 11648
  val Specification : Long  = 11392
  val JournalArticle : Long  = 12000
  val Professor : Long  = 13200
  val FullProfessor : Long  = 13205
  val Department : Long  = 10496
  val Course : Long  = 13568
  val Publication : Long  = 11264
  val UndergraduateStudent : Long  = 12928
  val AssociateProfessor : Long  = 13203
  val Article : Long  = 11904
  val TechnicalReport : Long  = 11936
  val ConferencePaper : Long  = 11968
  val Director : Long  = 13120
  val SystemsStaff : Long  = 13160
  val Program : Long  = 10752
  val TeachingAssistant : Long  = 12544
  val Software : Long  = 11520
  val ResearchGroup : Long  = 10880
  val UnofficialPublication : Long = 12032
  val GraduateCourse : Long = 13696
  val Organization : Long  = 10240
  val AssistantProfessor : Long  = 13206
  val Lecturer : Long  = 13208
  val College : Long  = 10368
  val ResearchAssistant : Long  = 13088
  val PostDoc : Long  = 13192
  val Research : Long  = 13824
  val Dean : Long  = 13204
  val University : Long  = 11008
  val Manual : Long  = 11776
  val Schedule : Long  = 9216
  val ClericalStaff : Long  = 13168

  /// Chargemnt du jeu de données non encodé
  val dict ="/LUBMInstances/lubm1.ttl"
  val lubmNonEncodedData = sc.textFile(dir +dict)
  println("PRINTING DATA ----- = "+lubmNonEncodedData.take(14).foreach(println))
  //  println(data)
  val lubmNonEncode = lubmNonEncodedData.map(x=>x.split(" ")).map(x=>(x(0), x(1), x(2)))
  //  conceptId2URI.persist()
  println("Data lubmNonEncode count = "+lubmNonEncode.count)
  println("lubmNonEncode println= "+lubmNonEncode.take(44).foreach(println))

  //// Nb concepts :

  val GraduateCourseT = conceptId2URI.filter(x=> x._3.equals(GraduateCourse)) // 13696 = 822
  println("Concepts GraduateCourse count = "+GraduateCourseT.count)


  /***************************** Statics : nb Properties********/
  val telephone : Long = 672
  val title : Long = 608
  val researchProject : Long = 772
  val doctoralDegreeFrom : Long = 787
  val mastersDegreeFrom : Long = 786
  val advisor : Long = 848
  val topObjectProperty : Long = 768
  val headOf : Long = 803
  val undergraduateDegreeFrom : Long = 785
  val publicationResearch : Long = 844
  val topDataProperty : Long = 1024
  val member : Long = 788
  val listedCourse : Long = 808
  val affiliateOf : Long = 820
  val tenured : Long = 824
  val teachingAssistantOf : Long = 796
  val emailAddress : Long = 736
  val _type : Long = 0
  val softwareDocumentation : Long = 804
  val subOrganizationOf : Long = 840
  val publicationAuthor : Long = 812
  val takesCourse : Long = 792
  val hasAlumnus : Long = 776
  val orgPublication : Long = 780
  val publicationDate : Long = 832
  val softwareVersion : Long = 816
  val worksFor : Long = 802
  val memberOf : Long = 800
  val teacherOf : Long = 828
  val affiliatedOrganizationOf : Long = 836
  val name : Long = 576
  val officeNumber : Long = 544
  val age : Long = 640
  val researchInterest : Long = 704
  val degreeFrom : Long = 784


  val telephoneP = conceptId2URI.filter(x=> x._1.equals(telephone)) // 13696 = 822
  println("Properties telephoneP count = "+telephoneP.count)

  /****************************** QUERY 1 *********************/
  /*
    * PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ub:<http://www.univ-mlv.fr/~ocure/lubm.owl#>
  PREFIX owl:<http://www.w3.org/2002/07/owl#>
  PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  SELECT ?x ?y ?z WHERE {
    ?x rdf:type ub:GraduateStudent .
    ?y rdf:type ub:University .
    ?z rdf:type ub:Department .
    ?x ub:memberOf ?z .
    ?z ub:subOrganizationOf ?y .
    ?x ub:undergraduateDegreeFrom ?y.
  }
    * */
  val rdfs = "http://www.w3.org/2000/01/rdf-schema";
  val ub = "http://www.univ-mlv.fr/~ocure/lubm.owl";
  val owl = "http://www.w3.org/2002/07/owl";
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns";

  val query1 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> \n}"
  val query3 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication> . \n\t?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor> <http://www.Department0.University0.edu/AssistantProfessor0> \n}"
  val query5 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person> . \n\t?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf> <http://www.Department0.University0.edu> \n}"
  val query6 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> \n}"
  val query10 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student> . \n\t?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse> <http://www.Department0.University0.edu/GraduateCourse0> \n}"
  val query14 = "SELECT ?x WHERE { \n\t?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> \n}"



  /* val dataFrame = sqlContext.sparqlQuery(dict, query1)
  dataFrame.show()*/

  /*val dataFrame6 = sqlContext.sparqlQuery(dict, query6)
  dataFrame6.show()*/

/*
  val name = Map("rdfs" -> rdfs , "ub"-> ub, "owl"->owl, "rdf"->rdf)1

  def quer+yT(typ:String, prop: String): String = name.get(typ).get + "#" + prop
  //val typeOf = lubmConceptData_.lookup.queryT("", "").head._1

  val Student : (Long, Long) = lubmConceptData_.lookup.(name.get("ub").get + "#" + "Student".head)
  val s1 = Student._1
  val s2 = Student._2

 // def borne(concept : (Long, Byte, Byte)) : (Long, Long) = {
   // val length = 44
   // val id = concept._1
  //}
  */

}
object Dct {
  def apply(sc: SparkContext, d: String): Dct = new Dct(sc, d)
}