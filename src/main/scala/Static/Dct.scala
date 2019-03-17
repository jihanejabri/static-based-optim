package Static

import org.apache.spark.SparkContext
import  org.apache.spark.sql.DataFrame

class Dct(sc : SparkContext, dir : String) {
  val lubmEncoded = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\LUBMInstances\\lubm1Encoded.txt\\"
  val lubmConcept = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_conceptsURL2Id.txt"
  val lubmProperties = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_propertiesURL2Id.txt"


  val propertiesId2Range = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_propertiesId2Range.txt"
  val propertiesId2Domain = "C:\\Users\\jmehd\\Desktop\\Projet Big data\\jeu d\\univ-bench_propertiesId2Domain.txt"

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

  val bench_propertiesId2RangeData = sc.textFile(propertiesId2Range)
  println("PRINTING DATA ----- = "+bench_propertiesId2RangeData.take(4).foreach(println))

  val bench_propertiesId2RangeData_ = bench_propertiesId2RangeData.map(x=>x.split("\t")).map(x=>(x(0), x(1)))

  println("Data propertiesId2Range count = "+bench_propertiesId2RangeData_.count)
  println("propertiesId2Range println= "+bench_propertiesId2RangeData_.take(4).foreach(println))

  val bench_propertiesId2DomainData = sc.textFile(propertiesId2Domain)
  println("PRINTING DATA ----- = "+bench_propertiesId2DomainData.take(4).foreach(println))

  val bench_propertiesId2DomainData_ = bench_propertiesId2DomainData.map(x=>x.split("\t")).map(x=>(x(0), x(1)))

  println("Data propertiesId2Domain count = "+bench_propertiesId2DomainData_.count)
  println("propertiesId2Domain println= "+bench_propertiesId2DomainData_.take(4).foreach(println))

  /****************************** QUERY ************************/

  val rdfs = "http://www.w3.org/2000/01/rdf-schema";
  val ub = "http://www.univ-mlv.fr/~ocure/lubm.owl";
  val owl = "http://www.w3.org/2002/07/owl";
  val rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns";

  val triples = lubmEncodedData.map(x =>(x(0), x(1), x(2)))
  val trilpesDF = triples.toDF("subject", "predicat", "object")
  triplesDF.persist()
  println("Triples = "+triples.count)

}
object Dct {
  def apply(sc: SparkContext, d: String): Dct = new Dct(sc, d)
}