package Static

import org.apache.spark.SparkContext

class Dct(sc : SparkContext, dir : String) {
  val lubmEncoded = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\LUBMInstances\\lubm1Encoded.txt\\"
  val lubmConcept = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_conceptsURL2Id.txt"
  val lubmProperties = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_propertiesURL2Id.txt"

  //  val data = sc.textFile(dir +lubmEncoded).map(x=>x.split(" "))
  val lubmEncodedData = sc.textFile(dir +lubmEncoded)
  println("PRINTING DATA ----- = "+lubmEncodedData.take(14).foreach(println))
  //  println(data)
  val conceptId2URI = lubmEncodedData.map(x=>x.split(" ")).map(x=>(x(0), x(1), x(2)))
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

  //  val  dct = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\LUBMInstances\\"
  //
  //  val triples = sc.texrFile(lubmEncoded).map(x=>x.split(" ")).map(w=>(x(0).toLong, x(1).toLong, x(2).toLong))
  //  val dataF = triples.map{case(s,o,p)=>(s,o,p)}.toDf("s","p","o")
  //  dataF.persist
  //  dataF.count*/
}
object Dct {
  def apply(sc: SparkContext, d: String): Dct = new Dct(sc, d)
}