package fr.inrae.msd.rdf

import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.jena.graph.Triple


case object PmidCidWork {
  val queryString = "select * where { " +
    "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle> . }"

  def getPMIDListFromReference_impl1(spark : SparkSession,referencePath: String): RDD[String] = {
    println(" ********************** \n\n")
    println(" IMPL1 ********************** Dataset[org.apache.jena.graph.Triple] **********************")
    println("\n\n **********************")
    val triples : RDD[Triple] = spark.rdf(Lang.TURTLE)(referencePath)
    val triplesDataset : Dataset[Triple] = triples.toDS()

    implicit val enc: Encoder[String] = Encoders.STRING

    val sparqlFrame =
      new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    triplesDataset.map(
      (triple  : Triple ) => {
        triple.getSubject.toString
      }
    ).rdd
    /*
    sparqlFrame.transform(triplesDataset).map(
      row  => row.get(0).toString
    ).rdd */
  }

  def getPMIDListFromReference_impl2(spark : SparkSession,referencePath: String): RDD[String] = {
    println(" ********************** \n\n")
    println(" IMPL2 ********************** RDD[BINDING] **********************")
    println("\n\n **********************")
    val triples = spark.rdf(Lang.TURTLE)(referencePath)
    val queryEngineFactory = new QueryEngineFactorySparqlify(spark)
    val qef1 = queryEngineFactory.create(triples)
    val qe = qef1.createQueryExecution(queryString)
    val rs = qe.execSelect()
    val result: ResultSetSpark = qe.execSelectSpark()
    val resultBindings: RDD[Binding] = result.getBindings // the bindings, i.e. mappings from vars to RDF resources

    resultBindings.map( (resb : Binding) => resb.get("s").getURI)
  }

  def getPMIDListFromReference_impl3(spark : SparkSession,referencePath: String): RDD[String] = {
    println(" ********************** \n\n")
    println(" IMPL3 ********************** triples.getSubjects **********************")
    println("\n\n **********************")
    val triples = spark.rdf(Lang.TURTLE)(referencePath)
    triples.getSubjects.map(_.toString)
  }

  def buildCitoDiscusses(mapPmidCid : RDD[(String,Seq[String])]) : RDD[Triple]  = {
    // create an empty model
    val model : Model = ModelFactory.createDefaultModel()
    model.setNsPrefix("cito", "http://purl.org/spar/cito/")
      .setNsPrefix("compound", "http://rdf.ncbi.nlm.nih.gov/pubchem/compound/")
      .setNsPrefix("reference", "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/")
    mapPmidCid.take(5).foreach( println )

    mapPmidCid.map {
          case (pmid : String,listCid : Seq[String]) =>listCid.map ( cid => {


            Triple.create(
              model.createResource(s"http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID$pmid").asNode(),
              model.createProperty("http://purl.org/spar/cito/discusses").asNode(),
              model.createResource(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID$cid").asNode()
            )
          })
     }.flatMap(
      x => x
    )
  }
}
