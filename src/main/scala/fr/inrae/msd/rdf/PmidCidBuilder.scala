package fr.inrae.msd.rdf

import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.jena.graph.Triple

/**
 * https://services.pfem.clermont.inrae.fr/gitlab/forum/metdiseasedatabase/-/blob/develop/app/build/import_PMID_CID.py
 * build/import_PMID_CID.py
 *
 * example using corese rdf4j : https://notes.inria.fr/s/OB038LBLV
 */
/*
To avoid => Exception in thread "main" java.lang.NoSuchMethodError: scala.runtime.Statics.releaseFence()V
can not extends App
 */
object PmidCidBuilder {

  import scopt.OParser

  case class Config(
                     rootMsdDirectory : String = "/rdf",
                     forumCategoryMsd : String = "forum",
                     forumDatabaseMsd : String = "PMID_CID",
                     pubchemCategoryMsd : String = "pubchem", //"/rdf/pubchem/compound-general/2021-11-23",
                     pubchemDatabaseMsd : String = "reference", // "/rdf/pubchem/reference/2021-11-23",
                     pubchemVersionMsd: Option[String] = None,
                     implGetPMID: Int = 0, /* 0 : Dataset[Triple], 1 : [RDD[Binding], 2 : Triples.getSubject */
                     referenceUriPrefix: String = "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID",
                     packSize : Int = 300,
                     apiKey : Option[String] = Some("30bc501ba6ab4cba2feedffb726cbe825c0a"),
                     timeout : Int = 1200,
                     verbose: Boolean = false,
                     debug: Boolean = false)

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("msd-metdisease-database-pmid-cid-builder"),
      head("msd-metdisease-database-pmid-cid-builder", "1.0"),
      opt[String]('d', "rootMsdDirectory")
        .optional()
        .valueName("<rootMsdDirectory>")
        .action((x, c) => c.copy(rootMsdDirectory = x))
        .text("versionMsd : release of reference/pubchem database"),
      opt[String]('r', "versionMsd")
        .optional()
        .valueName("<versionMsd>")
        .action((x, c) => c.copy(pubchemVersionMsd = Some(x)))
        .text("versionMsd : release of reference/pubchem database"),
      opt[Int]('i',"implGetPMID")
        .optional()
        .action({ case (r, c) => c.copy(implGetPMID = r) })
        .validate(x =>
          if ((x >= 0) && (x <=2)) success
          else failure("Value <implementation> must be >0"))
        .valueName("<implGetPMID>")
        .text("implementation to get PMID subject from reference - 0 : Dataset[Triple], 1 : [RDD[Binding], 2 : Triples.getSubject."),
      opt[Int]("packSize")
        .optional()
        .action({ case (r, c) => c.copy(packSize = r) })
        .validate(x =>
          if (x > 0) success
          else failure("Value <packSize> must be >0"))
        .valueName("<packSize>")
        .text("packSize to request pmid/cid eutils/elink API."),
      opt[String]("apiKey")
        .optional()
        .action({ case (r, c) => c.copy(apiKey = Some(r)) })
        .valueName("<apiKey>")
        .text("apiKey to request pmid/cid eutils/elink API."),
      opt[Int]("timeout")
        .optional()
        .action({ case (r, c) => c.copy(timeout = r) })
        .validate(x =>
          if (x > 0) success
          else failure("Value <timeout> must be >0"))
        .valueName("<timeout>")
        .text("timeout to manage error request pmid/cid eutils/elink API."),
      opt[Unit]("verbose")
        .optional()
        .action((_, c) => c.copy(verbose = true))
        .text("verbose is a flag"),
      opt[Unit]("debug")
        .hidden()
        .action((_, c) => c.copy(debug = true))
        .text("this option is hidden in the usage text"),

      help("help").text("prints this usage text"),
      note("some notes." + sys.props("line.separator")),
      checkConfig(_ => success)
    )
  }
  val spark = SparkSession
    .builder()
    .appName("msd-metdisease-database-pmid-cid-builder")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", String.join(
      ", ",
      "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
      "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator",
      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"))
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        // do something
        println(config)
        build(
          config.rootMsdDirectory,
          config.forumCategoryMsd,
          config.forumDatabaseMsd,
          config.pubchemCategoryMsd,
          config.pubchemDatabaseMsd,
          config.pubchemVersionMsd match {
            case Some(version) => version
            case None => MsdUtils(
              rootDir=config.rootMsdDirectory,
              category=config.pubchemCategoryMsd,
              database=config.pubchemDatabaseMsd,spark=spark).getLastVersion()
          },
          config.implGetPMID,
          config.referenceUriPrefix,
          config.packSize,
          config.apiKey match {
            case Some(apiK) => apiK
            case None => ""
          },
          config.timeout,
          config.verbose,
          config.debug)
      case _ =>
        // arguments are bad, error message will have been displayed
        System.err.println("exit with error.")
    }
  }

  def build(
             rootMsdDirectory : String,
             forumCategoryMsd : String,
             forumDatabaseMsd : String,
             categoryMsd : String,
             databaseMsd : String,
             versionMsd: String,
             implGetPMID: Int,
             referenceUriPrefix: String,
             packSize : Int,
             apiKey : String,
             timeout : Int,
             verbose: Boolean,
             debug: Boolean) : Unit = {
    println("============== Main Build ====================")
    println(s"categoryMsd=$categoryMsd,databaseMsd=$databaseMsd,versionMsd=$versionMsd")
    println("==============  getPMIDListFromReference ====================")
    val listReferenceFileNames = MsdUtils(
      rootDir=rootMsdDirectory,
      category=categoryMsd,
      database=databaseMsd,
      spark=spark).getListFiles(versionMsd,".*_type.*2\\.ttl")

    println("================listReferenceFileNames==============")
    println(listReferenceFileNames)

    if (listReferenceFileNames.length<=0) {
      println(s"None reference file in $rootMsdDirectory/$categoryMsd/$databaseMsd/$versionMsd")
      spark.close()
      System.exit(0)
    }

    val pmids : RDD[String] = spark.sparkContext.union(listReferenceFileNames.map(
      referenceFileName => implGetPMID match {
        case 1 => PmidCidWork.getPMIDListFromReference_impl2(spark,referenceFileName)
        case 2 => PmidCidWork.getPMIDListFromReference_impl3(spark,referenceFileName)
        case _ => PmidCidWork.getPMIDListFromReference_impl1(spark,referenceFileName)
      }))
      val numberOfPmid = pmids.count()

    /* repartition to optimize elink http request */
    val pmidsRep = pmids.repartition(numPartitions = (numberOfPmid / packSize).toInt + 1) /* repartition to call elink process */

    println(s"================PMID List ($numberOfPmid)==============")
    println(pmids.take(10).slice(1,10)+"...")

    val pmidCitoDiscussesCid = EUtils.elink(apikey=apiKey,dbFrom="pubmed", db="pccompound",pmidsRep)
    println(s"================pmidCitoDiscussesCid (${pmidCitoDiscussesCid.count()})==============")
//    println(pmidCitoDiscussesCid.take(10).mkString(",")+"...")

    println(" ========== save pmid list without success elink request ========")
    //val lProblemPmid = pmids diff pmidCitoDiscussesCid.keys.toSeq
    //println(" pmid problem:" + lProblemPmid.length)
    //println(s"================ Write Turtle $rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionMsd/error_with_pmid ==============")
/*
    MsdUtils(
      rootDir=rootMsdDirectory,
      category=forumCategoryMsd,
      database=forumDatabaseMsd,
      spark=spark).writeDataframeAsTxt(spark,lProblemPmid,versionMsd,"error_with_pmid")*/
    val triples_asso_pmid_cid : RDD[Triple] = PmidCidWork.buildCitoDiscusses(pmidCitoDiscussesCid)
   // println(s"================ Write Turtle $rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionMsd/pmid_cid.ttl ==============")
   /* MsdUtils(
      rootDir=rootMsdDirectory,
      category=forumCategoryMsd,
      database=forumDatabaseMsd,
      spark=spark).writeRdf(model,Lang.TURTLE,versionMsd,"pmid_cid.ttl")
*/
    import net.sansa_stack.rdf.spark.io._
    triples_asso_pmid_cid.saveAsNTriplesFile(s"$rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionMsd/pmid_cid.ttl",mode=SaveMode.Overwrite) //.take(5))
    spark.close()
  }

}
