package Static

import org.apache.jena.rdf.model.ModelFactory

class StaticBasedOptim(val dbSource: String) {
  val model = ModelFactory.createDefaultModel()
  def load() = model.read(dbSource)
}
