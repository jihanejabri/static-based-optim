package Static

import org.apache.spark.SparkContext

class Dct(sc : SparkContext, dir : String) {
  val lubmEncoded = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\LUBMInstances\\lubm1Encoded.txt\\"
  val lubmConcept = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_conceptsURL2Id.txt"
  val lubmProperties = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_propertiesURL2Id.txt"

  val data = sc.textFile(dir +lubmEncoded).map(x=>x.split(""))
  val conceptId2URI = data.map(x=>(x(1), x(0), x(2)))
  conceptId2URI.persist()
  println("Data = "+conceptId2URI.count)

  val concepts = sc.textFile(dir + lubmConcept).map(x=>x.split(""))
  val conceptId2URII = concepts.map(x=>(x(1), x(0), x(2)))
  conceptId2URII.persist()
  println("conceptId2URII = "+conceptId2URII.count)

  val conceptsURII2Id = concepts.map(x=>(x(0),x(1)))
  conceptsURII2Id.persist
  println("conceptsURII2Id = "+conceptsURII2Id.count)

 /* val properties = sc.textFile(dir+ lubmProperties).map(x=>x.split(" "))
  val propertiesId2URI = properties.map(x=>(x(1), x(0), x(2)))
  propertiesId2URI.persist()
  println("propertiesId2URI = "+propertiesId2URI.count)*/
  /*
  val  dct = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\LUBMInstances\\"

  val triples = sc.texrFile(lubmEncoded).map(x=>x.split(" ")).map(w=>(x(0).toLong, x(1).toLong, x(2).toLong))
  val dataF = triples.map{case(s,o,p)=>(s,o,p)}.toDf("s","p","o")
  dataF.persist
  dataF.count*/
}
object Dct {
  def apply(sc: SparkContext, d: String): Dct = new Dct(sc, d)
}