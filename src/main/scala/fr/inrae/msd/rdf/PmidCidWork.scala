package fr.inrae.msd.rdf

import fr.inrae.msd.rdf.EUtils.ElinkData
import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.query.spark.api.domain.ResultSetSpark
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.rdf.spark.io.RDFReader
import net.sansa_stack.rdf.spark.model.TripleOperations
import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

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

    triplesDataset.map(
      (triple  : Triple ) => {
        triple.getSubject.toString
      }
    ).rdd

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

  def getPMIDListFromReference_impl4(spark : SparkSession,referencePath: String): RDD[String] = {
    println(" ********************** \n\n")
    println(" IMPL4 ********************** sparqlFrame **********************")
    println("\n\n **********************")

    val triples : RDD[Triple] = spark.rdf(Lang.TURTLE)(referencePath)
    val triplesDataset : Dataset[Triple] = triples.toDS()

    val sparqlFrame =
      new SparqlFrame()
        .setSparqlQuery(queryString)
        .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    implicit val enc: Encoder[String] = Encoders.STRING

    sparqlFrame.transform(triplesDataset).map(
        row  => row.get(0).toString
    ).rdd
  }

  def buildCitoDiscusses(mapPmidCid : Dataset[(String,Seq[String])]) : Dataset[Triple]  = {
    implicit val nodeTripleEncoder: Encoder[Seq[Triple]] = Encoders.kryo(classOf[Seq[Triple]])
    implicit val nodeSeqTripleEncoder: Encoder[Triple] = Encoders.kryo(classOf[Triple])
    mapPmidCid.map {
          case (pmid : String,listCid : Seq[String]) =>listCid.map ( cid => {

            Triple.create(
              NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID$pmid"),
              NodeFactory.createURI("http://purl.org/spar/cito/discusses"),
              NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID$cid")
            )
          })
     }.flatMap(
      x => x
    )
  }

  def buildContributors(spark : SparkSession,contributors : Dataset[ElinkData]) : Dataset[Triple]  = {

    implicit val nodeTripleEncoder: Encoder[Triple] = Encoders.kryo(classOf[Triple])
    spark.emptyDataset[Triple].union(
      contributors.flatMap(
      elinkData => {
        elinkData.cids.flatMap(cid => {
          val subject = NodeFactory.createURI(s"https://forum.semantic-metabolomics.org/mention/PMID${elinkData.pmid}_CID$cid")
          Seq(
            Triple.create(
              subject,
              NodeFactory.createURI("http://purl.obolibrary.org/obo/IAO_0000136"),
              NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/reference/${elinkData.pmid}")
            ),
            Triple.create(
              subject,
              NodeFactory.createURI("http://purl.org/spar/cito/isCitedAsDataSourceBy"),
              NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/$cid")
            )) ++
            elinkData.contributorType.map(
              contributorType =>
                Triple.create(
                  subject,
                  NodeFactory.createURI("http://purl.org/dc/terms/contributor"),
                  NodeFactory.createURI(contributorType)
                )
            )
        })
      }
    ))
  }
}
