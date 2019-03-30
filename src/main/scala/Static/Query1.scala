package Static
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.util.FileManager
import org.apache.spark.SparkContext;

class Query1(sc: SparkContext) {
  val model : Model = FileManager.get().loadModel("")
  val query1 = "" + "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                    "PREFIX ub:<http://www.univ-mlv.fr/~ocure/lubm.owl#> " +
                    "SELECT ?x WHERE { " +
                    "?x rdf:type ub:GraduateStudent ."+
                    "?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>.}"

  val query = QueryFactory.create(query1)
  query.setPrefix("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns")
  query.setPrefix("ub","http://www.univ-mlv.fr/~ocure/lubm.owl")

  /*
  var queryExec: QueryExecution = QueryExecutionFactory.create(query, model)
  val results = queryExec.execSelect()
 */

  /*
   private var sparqlQuery1 = """
                                 PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
                                 PREFIX ub:<http://www.univ-mlv.fr/~ocure/lubm.owl#>
                                 PREFIX owl:<http://www.w3.org/2002/07/owl#>
                                 PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                 SELECT ?x WHERE {
                                   ?x rdf:type ub:GraduateStudent .
                                   ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>.}"""

    val query = QueryFactory.create(sparqlQuery1, Syntax.syntaxARQ)
    val results = queryExec.execSelect
    //val parameterizedSparqlString = new parameterizedSparqlString(query.toString(), new QuerySolutionMap())
    var queryExec: QueryExecution = QueryExecutionFactory.create(query,/**model ou dataset**/)
  */
  //val results = queryExec.execSelect
 // var queryExec: QueryExecution = QueryExecutionFactory.create(query, /*dataset**/)

  /*
private var sparqlQuery1 = """
                               PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
                               PREFIX ub:<http://www.univ-mlv.fr/~ocure/lubm.owl#>
                               PREFIX owl:<http://www.w3.org/2002/07/owl#>
                               PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                               SELECT ?x WHERE {
                                 ?x rdf:type ub:GraduateStudent .
                                 ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>.}"""

  sparqlQuery1.setNsPrefix("rdfs" , "http://www.w3.org/2000/01/rdf-schema");
  qs.setNsPrefix("ub" , "http://www.univ-mlv.fr/~ocure/lubm.owl");
  qs.setNsPrefix("owl" , "http://www.w3.org/2002/07/owl");
  qs.setNsPrefix("rdf" , "http://www.w3.org/1999/02/22-rdf-syntax-ns");

   print("Running as a query" + qs.asQuery())*/
}

object Query1 {
  def apply(sc: SparkContext): Query1 = new Query1(sc)
}