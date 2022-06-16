package fr.inrae.msd.rdf

import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.semantic.QuerySystem
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import net.sansa_stack.rdf.spark.partition.RDFPartition
import org.apache.jena.query.ResultSetFormatter
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case object PmidCidWork {

  def saveMethod(spark : SparkSession,referencePath: String): Unit = {

    val queryString = "select * where { " +
      "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle> . }"

    val triples = spark.rdf(Lang.TURTLE)(referencePath)
    val queryEngineFactory = new QueryEngineFactorySparqlify(spark)
    val qef1 = queryEngineFactory.create(triples)
    val qe = qef1.createQueryExecution(queryString)
    val rs = qe.execSelect()
    val result: ResultSetSpark = qe.execSelectSpark()
    val resultBindings: RDD[Binding] = result.getBindings // the bindings, i.e. mappings from vars to RDF resources
    val resultVars: Seq[Var] = result.getResultVars

    println("=================================saveMethod=============================")
    ResultSetFormatter.asText(rs)
    println(resultBindings.collect())
    println(resultVars)
    println("=================================END=============================")
  }

  def getPMIDListFromReference(spark : SparkSession,referencePath: String): Seq[String] = {

    val triples = spark.rdf(Lang.TURTLE)(referencePath)
    val triplesDataset = triples.toDS()
    val queryString = "select * where { " +
      "?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle> . }"

    val sparqlFrame =
      new SparqlFrame()
      .setSparqlQuery(queryString)
      .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    println("==================  START ========================")
    val res = sparqlFrame.transform(triplesDataset).collect().map(
      row => row.get(0).toString
    ).toSeq
    println(res.mkString(","))
    println("==================  END ========================")
    res
    // res.map(row => row.toString())
   // val str : String = spark.read.text(referencePath).collect().map(row => row.mkString("")).mkString("\n")
  }

  def buildCitoDiscusses(mapPmidCid : Map[String,Seq[String]]) : Unit  = {

  }
}
