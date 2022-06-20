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
import org.apache.spark.sql.{Dataset, SparkSession}

case object PmidCidWork {
  val queryString = "select * where { " +
    "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle> . }"

  def getPMIDListFromReference_impl1(spark : SparkSession,referencePath: String): Seq[String] = {
    println(" ********************** \n\n")
    println(" IMPL1 ********************** Dataset[org.apache.jena.graph.Triple] **********************")
    println("\n\n **********************")
    val triples = spark.rdf(Lang.TURTLE)(referencePath)
    val triplesDataset : Dataset[org.apache.jena.graph.Triple] = triples.toDS()

    val sparqlFrame =
      new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    sparqlFrame.transform(triplesDataset).collect().map(
      row => row.get(0).toString
    ).toSeq
  }

  def getPMIDListFromReference_impl2(spark : SparkSession,referencePath: String): Seq[String] = {
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
    val resultVars: Seq[Var] = result.getResultVars

    resultBindings.collect().map( (resb : Binding) => resb.get("s").getURI).toSeq
  }

  def getPMIDListFromReference_impl3(spark : SparkSession,referencePath: String): Seq[String] = {
    println(" ********************** \n\n")
    println(" IMPL3 ********************** triples.getSubjects **********************")
    println("\n\n **********************")
    val triples = spark.rdf(Lang.TURTLE)(referencePath)
    triples.getSubjects.collect().map(_.toString)
  }

  def buildCitoDiscusses(mapPmidCid : Map[String,Seq[String]]) : Model  = {
    // create an empty model
    val model : Model = ModelFactory.createDefaultModel()

    model.setNsPrefix("cito", "http://purl.org/spar/cito/")
      .setNsPrefix("compound", "http://rdf.ncbi.nlm.nih.gov/pubchem/compound/")
      .setNsPrefix("reference", "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/")

    mapPmidCid.foreach {
      case (pmid,listCid) =>listCid.foreach( cid => {
          model
            .createResource(s"http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID$pmid")
            .addProperty(
              model.createProperty("http://purl.org/spar/cito/discusses"),
              model.createResource(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID$cid"))
      })}
    model
  }
}
