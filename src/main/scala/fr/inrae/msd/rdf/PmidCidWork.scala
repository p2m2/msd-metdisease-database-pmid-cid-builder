package fr.inrae.msd.rdf

import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

case object PmidCidWork {
  def getPMIDListFromReference(spark : SparkSession,referencePath: String): Seq[String] = {
    val lang = Lang.TURTLE

    val triplesDataset = spark.rdf(lang)(referencePath).toDS()
    val queryString = "select ?s where { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle> . }"

    val sparqlFrame = new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    val res = sparqlFrame.transform(triplesDataset).collect()
    res.map(row => row.toString())
   // val str : String = spark.read.text(referencePath).collect().map(row => row.mkString("")).mkString("\n")
  }

  def buildCitoDiscusses(mapPmidCid : Map[String,Seq[String]]) : Unit  = {

  }
}
