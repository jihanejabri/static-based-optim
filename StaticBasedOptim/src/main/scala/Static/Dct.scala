package Static

import org.apache.spark.SparkContext

class Dct(sc : SparkContext, dir : String) {
  val lubmEncoded = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\LUBMInstances\\lubm1Encoded.txt\\"
  val lubmConcept = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_conceptsURL2Id"
  val lubmProperties = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_propertiesURL2Id"

  val data = sc.textFile(dir +lubmEncoded).map(x=>x.split(""))
  val conceptId2URI = data.map(x=>(x(1), x(0), x(2)))
  conceptId2URI.persist
  conceptId2URI.count
  println("Data = "+conceptId2URI.count)
  println("Data = "+data)
  /*val concepts = sc.textFile(dct + lubmConcept).map(x=>x.split(""))
  val conceptId2URI = concepts.map(x=>(x(1), x(0), x(2)))
  conceptId2URI.persist()
  conceptId2URI.count*/

 /* val properties = sc.textFile(dct+ lubmProperties).map(x=>x.split(" "))
  val propertiesId2URI = properties.map(x=>(x(1), x(0), x(2)))
  propertiesId2URI.persist()
  propertiesId2URI.count*/
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