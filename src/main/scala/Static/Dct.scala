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

 /* val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val trilpesDF = conceptId2URI.toDF("subject", "predicat", "object")
  println("Triples = "+trilpesDF.count)
  println("trilpesDF println= "+trilpesDF.take(4).foreach(println))*/

  /***************************** Statics : nb Concepts*********/
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

  /// Chargement du jeu de données non encodés
  val dict ="/LUBMInstances/lubm1.ttl"
  val lubmNonEncodedData = sc.textFile(dir +dict)
  println("PRINTING DATA ----- = "+lubmNonEncodedData.take(14).foreach(println))
  //  println(data)
  val lubmNonEncode = lubmNonEncodedData.map(x=>x.split(" ")).map(x=>(x(0), x(1), x(2)))
  //  conceptId2URI.persist()
  println("Data lubmNonEncode count = "+lubmNonEncode.count)
  println("lubmNonEncode println= "+lubmNonEncode.take(44).foreach(println))

  val sqlContext= new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val trilpeDF = lubmNonEncode.toDF("subject", "predicat", "object")
  println("Triples = "+trilpeDF.count)
  println("trilpesDF println= "+trilpeDF.take(4).foreach(println))

  //// Nb concepts :

  ///Work
  val WorkT = conceptId2URI.filter(x=> x._3.equals(Work)) //1
  println("Concepts Work count = "+WorkT.count)
  ///Faculty
  val FacultyT = conceptId2URI.filter(x=> x._3.equals(Faculty)) //3
  println("Concepts Faculty count = "+FacultyT.count)
  ///Person
  val PersonT = conceptId2URI.filter(x=> x._3.equals(Person)) //7
  println("Concepts Person count = "+PersonT.count)
  ///Student
  val StudentT = conceptId2URI.filter(x=> x._3.equals(Student)) //0
  println("Concepts Student count = "+StudentT.count)
  ///Institute
  val GraduateStudentT = conceptId2URI.filter(x=> x._3.equals(GraduateStudent)) //1889
  println("Concepts GraduateStudent count = "+GraduateStudentT.count)
  ///Employee
  val EmployeeT = conceptId2URI.filter(x=> x._3.equals(Employee)) //0
  println("Concepts Employee count = "+EmployeeT.count)
  ///VisitingProfessor
  val VisitingProfessorT = conceptId2URI.filter(x=> x._3.equals(VisitingProfessor)) //1
  println("Concepts VisitingProfessor count = "+VisitingProfessorT.count)
  ///Chair
  val ChairT = conceptId2URI.filter(x=> x._3.equals(Chair)) //0
  println("Concepts Chair count = "+ChairT.count)
  ///Thing
  val ThingT = conceptId2URI.filter(x=> x._3.equals(Thing)) //1
  println("Concepts Thing count = "+ThingT.count)
  ///AdministrativeStaff
  val AdministrativeStaffT = conceptId2URI.filter(x=> x._3.equals(AdministrativeStaff)) //2
  println("Concepts AdministrativeStaff count = "+AdministrativeStaffT.count)
  ///Book
  val BookT = conceptId2URI.filter(x=> x._3.equals(Book)) //0
  println("Concepts Book count = "+BookT.count)
  ///Specification
  val SpecificationT = conceptId2URI.filter(x=> x._3.equals(Specification)) //0
  println("Concepts Specification count = "+SpecificationT.count)
  ///JournalArticle
  val JournalArticleT = conceptId2URI.filter(x=> x._3.equals(JournalArticle)) //0
  println("Concepts JournalArticle count = "+JournalArticleT.count)
  ///Professor
  val ProfessorT = conceptId2URI.filter(x=> x._3.equals(Professor)) //2
  println("Concepts Professor count = "+ProfessorT.count)
  ///FullProfessor
  val FullProfessorT = conceptId2URI.filter(x=> x._3.equals(FullProfessor)) //125
  println("Concepts FullProfessor count = "+FullProfessorT.count)
  ///Department
  val DepartmentT = conceptId2URI.filter(x=> x._3.equals(Department)) //15
  println("Concepts Department count = "+DepartmentT.count)
  ///Publication
  val PublicationT = conceptId2URI.filter(x=> x._3.equals(Publication)) //6000
  println("Concepts Publication count = "+PublicationT.count)
  ///UndergraduateStudent
  val UndergraduateStudentT = conceptId2URI.filter(x=> x._3.equals(UndergraduateStudent)) //5916
  println("Concepts UndergraduateStudent count = "+UndergraduateStudentT.count)
  ///AssociateProfessor
  val AssociateProfessorT = conceptId2URI.filter(x=> x._3.equals(AssociateProfessor)) //181
  println("Concepts AssociateProfessor count = "+AssociateProfessorT.count)
  ///Article
  val ArticleT = conceptId2URI.filter(x=> x._3.equals(Article)) //1
  println("Concepts Article count = "+ArticleT.count)
  ///TechnicalReport
  val TechnicalReportT = conceptId2URI.filter(x=> x._3.equals(TechnicalReport)) //23
  println("Concepts TechnicalReport count = "+TechnicalReportT.count)
  ///ConferencePaper
  val ConferencePaperT = conceptId2URI.filter(x=> x._3.equals(ConferencePaper)) //7
  println("Concepts ConferencePaper count = "+ConferencePaperT.count)
  ///Director
  val DirectorT = conceptId2URI.filter(x=> x._3.equals(Director)) //0
  println("Concepts Director count = "+DirectorT.count)
  ///SystemsStaff
  val SystemsStaffT = conceptId2URI.filter(x=> x._3.equals(SystemsStaff)) //0
  println("Concepts SystemsStaff count = "+SystemsStaffT.count)
  ///Program
  val ProgramT = conceptId2URI.filter(x=> x._3.equals(Program)) //0
  println("Concepts Program count = "+ProgramT.count)
  ///TeachingAssistant
  val TeachingAssistantT = conceptId2URI.filter(x=> x._3.equals(TeachingAssistant)) //408
  println("Concepts TeachingAssistant count = "+TeachingAssistantT.count)
  ///Software
  val SoftwareT = conceptId2URI.filter(x=> x._3.equals(Software)) //0
  println("Concepts Software count = "+SoftwareT.count)
  ///ResearchGroup
  val ResearchGroupT = conceptId2URI.filter(x=> x._3.equals(ResearchGroup)) //224
  println("Concepts ResearchGroup count = "+ResearchGroupT.count)
  ///UnofficialPublication
  val UnofficialPublicationT = conceptId2URI.filter(x=> x._3.equals(UnofficialPublication)) //1
  println("Concepts UnofficialPublication count = "+UnofficialPublicationT.count)
  ///GraduateCourse
  val GraduateCourseT = conceptId2URI.filter(x=> x._3.equals(GraduateCourse)) // 13696 = 822
  println("Concepts GraduateCourse count = "+GraduateCourseT.count)
  ///Organization
  val OrganizationT = conceptId2URI.filter(x=> x._3.equals(Organization)) //7
  println("Concepts Organization count = "+OrganizationT.count)
  ///AssistantProfessor
  val AssistantProfessorT = conceptId2URI.filter(x=> x._3.equals(AssistantProfessor)) //147
  println("Concepts AssistantProfessor count = "+AssistantProfessorT.count)
  ///Lecturer
  val LecturerT = conceptId2URI.filter(x=> x._3.equals(Lecturer)) //94
  println("Concepts Lecturer count = "+LecturerT.count)
  ///College
  val CollegeT = conceptId2URI.filter(x=> x._3.equals(College)) //5
  println("Concepts College count = "+CollegeT.count)
  ///ResearchAssistant
  val ResearchAssistantT = conceptId2URI.filter(x=> x._3.equals(ResearchAssistant)) //547
  println("Concepts ResearchAssistant count = "+ResearchAssistantT.count)
  ///AssistantProfessor
  val PostDocT = conceptId2URI.filter(x=> x._3.equals(PostDoc)) //0
  println("Concepts PostDoc count = "+PostDocT.count)
  ///Research
  val ResearchT = conceptId2URI.filter(x=> x._3.equals(Research)) //1
  println("Concepts Research count = "+ResearchT.count)
  ///Dean
  val DeanT = conceptId2URI.filter(x=> x._3.equals(Dean)) //1
  println("Concepts Dean count = "+DeanT.count)
  ///University
  val UniversityT = conceptId2URI.filter(x=> x._3.equals(University)) //3510
  println("Concepts University count = "+UniversityT.count)
  ///Manual
  val ManualT = conceptId2URI.filter(x=> x._3.equals(Manual)) //1
  println("Concepts Manual count = "+ManualT.count)
  ///Schedule
  val ScheduleT = conceptId2URI.filter(x=> x._3.equals(Schedule)) //6
  println("Concepts Schedule count = "+ScheduleT.count)
  ///ClericalStaff
  val ClericalStaffT = conceptId2URI.filter(x=> x._3.equals(ClericalStaff)) //1
  println("Concepts ClericalStaff count = "+ClericalStaffT.count)


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

  ///telephone
  val telephoneP = conceptId2URI.filter(x=> x._2.equals(telephone)) // 8330
  println("Properties telephone count = "+telephoneP.count)
  ///title
  val titleP = conceptId2URI.filter(x=> x._2.equals(title)) //0
  println("Properties title count = "+titleP.count)
  ///researchProject
  val researchProjectP = conceptId2URI.filter(x=> x._2.equals(researchProject)) //0
  println("Properties researchProject count = "+researchProjectP.count)
  ///doctoralDegreeFrom
  val doctoralDegreeFromP = conceptId2URI.filter(x=> x._2.equals(doctoralDegreeFrom)) //540
  println("Properties doctoralDegreeFrom count = "+doctoralDegreeFromP.count)
  ///mastersDegreeFrom
  val mastersDegreeFromP = conceptId2URI.filter(x=> x._2.equals(mastersDegreeFrom)) //540
  println("Properties mastersDegreeFrom count = "+mastersDegreeFromP.count)
  ///advisor
  val advisorP = conceptId2URI.filter(x=> x._2.equals(advisor)) //3101
  println("Properties advisor count = "+advisorP.count)
  ///topObjectProperty
  val topObjectPropertyP = conceptId2URI.filter(x=> x._2.equals(topObjectProperty)) //0
  println("Properties topObjectProperty count = "+topObjectPropertyP.count)
  ///headOf
  val headOfP = conceptId2URI.filter(x=> x._2.equals(headOf)) //15
  println("Properties headOf count = "+headOfP.count)
  ///undergraduateDegreeFrom
  val undergraduateDegreeFromP = conceptId2URI.filter(x=> x._2.equals(undergraduateDegreeFrom)) //2414
  println("Properties undergraduateDegreeFrom count = "+undergraduateDegreeFromP.count)
  ///publicationResearch
  val publicationResearchP = conceptId2URI.filter(x=> x._2.equals(publicationResearch)) //0
  println("Properties publicationResearch count = "+publicationResearchP.count)
  ///topDataProperty
  val topDataPropertyP = conceptId2URI.filter(x=> x._2.equals(topDataProperty)) //0
  println("Properties topDataProperty count = "+topDataPropertyP.count)
  ///member
  val memberP = conceptId2URI.filter(x=> x._2.equals(member)) //0
  println("Properties member count = "+memberP.count)
  ///listedCourse
  val listedCourseP = conceptId2URI.filter(x=> x._2.equals(listedCourse)) //0
  println("Properties listedCourse count = "+listedCourseP.count)
  ///affiliateOf
  val affiliateOfP = conceptId2URI.filter(x=> x._2.equals(affiliateOf)) //0
  println("Properties affiliateOf count = "+affiliateOfP.count)
  ///tenured
  val tenuredP = conceptId2URI.filter(x=> x._2.equals(tenured)) //0
  println("Properties tenured count = "+tenuredP.count)
  ///teachingAssistantOf
  val teachingAssistantOfP = conceptId2URI.filter(x=> x._2.equals(teachingAssistantOf)) //407
  println("Properties teachingAssistantOf count = "+teachingAssistantOfP.count)
  ///emailAddress
  val typeP = conceptId2URI.filter(x=> x._2.equals(_type)) //20659
  println("Properties type count = "+typeP.count)
  ///softwareDocumentation
  val softwareDocumentationP = conceptId2URI.filter(x=> x._2.equals(softwareDocumentation)) //0
  println("Properties softwareDocumentation count = "+softwareDocumentationP.count)
  ///subOrganizationOf
  val subOrganizationOfP = conceptId2URI.filter(x=> x._2.equals(subOrganizationOf)) //239
  println("Properties subOrganizationOf count = "+subOrganizationOfP.count)
  ///publicationAuthor
  val publicationAuthorP = conceptId2URI.filter(x=> x._2.equals(publicationAuthor)) //10634
  println("Properties publicationAuthor count = "+publicationAuthorP.count)
  ///takesCourse
  val takesCourseP = conceptId2URI.filter(x=> x._2.equals(takesCourse)) //21489
  println("Properties takesCourse count = "+takesCourseP.count)
  ///hasAlumnus
  val hasAlumnusP = conceptId2URI.filter(x=> x._2.equals(hasAlumnus)) //0
  println("Properties hasAlumnus count = "+hasAlumnusP.count)
  ///orgPublication
  val orgPublicationP = conceptId2URI.filter(x=> x._2.equals(orgPublication)) //0
  println("Properties orgPublication count = "+orgPublicationP.count)
  ///publicationDate
  val publicationDateP = conceptId2URI.filter(x=> x._2.equals(publicationDate)) //0
  println("Properties publicationDate count = "+publicationDateP.count)
  ///softwareVersion
  val softwareVersionP = conceptId2URI.filter(x=> x._2.equals(softwareVersion)) //0
  println("Properties softwareVersion count = "+softwareVersionP.count)
  ///worksFor
  val worksForP = conceptId2URI.filter(x=> x._2.equals(worksFor)) //540
  println("Properties worksFor count = "+worksForP.count)
  ///memberOf
  val memberOfP = conceptId2URI.filter(x=> x._2.equals(memberOf)) //7790
  println("Properties memberOf count = "+memberOfP.count)
  ///teacherOf
  val teacherOfP = conceptId2URI.filter(x=> x._2.equals(teacherOf)) //1627
  println("Properties teacherOf count = "+teacherOfP.count)
  ///affiliatedOrganizationOf
  val affiliatedOrganizationOfP = conceptId2URI.filter(x=> x._2.equals(affiliatedOrganizationOf)) //0
  println("Properties affiliatedOrganizationOf count = "+affiliatedOrganizationOfP.count)
  ///name
  val nameP = conceptId2URI.filter(x=> x._2.equals(name)) //15972
  println("Properties name count = "+nameP.count)
  ///officeNumber
  val officeNumberP = conceptId2URI.filter(x=> x._2.equals(officeNumber)) //0
  println("Properties officeNumber count = "+officeNumberP.count)
  ///age
  val ageP = conceptId2URI.filter(x=> x._2.equals(age)) //0
  println("Properties age count = "+ageP.count)
  ///researchInterest
  val researchInterestP = conceptId2URI.filter(x=> x._2.equals(researchInterest)) //447
  println("Properties researchInterest count = "+researchInterestP.count)
  ///degreeFrom
  val degreeFromP = conceptId2URI.filter(x=> x._2.equals(degreeFrom)) //0
  println("Properties degreeFrom count = "+degreeFromP.count)

  /****************************************************/

}
object Dct {
  def apply(sc: SparkContext, d: String): Dct = new Dct(sc, d)
}