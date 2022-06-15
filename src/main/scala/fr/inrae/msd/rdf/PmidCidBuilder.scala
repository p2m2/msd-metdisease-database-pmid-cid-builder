package fr.inrae.msd.rdf

import org.apache.spark.sql.SparkSession


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
                     maxTriplesByFiles: Int = 5000000,
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
      opt[Int]("maxTriplesByFiles")
        .optional()
        .action({ case (r, c) => c.copy(maxTriplesByFiles = r) })
        .validate(x =>
          if (x > 0) success
          else failure("Value <maxTriplesByFiles> must be >0"))
        .valueName("<maxTriplesByFiles>")
        .text("maxTriplesByFiles to write pmid/cid turtle files."),
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
          config.maxTriplesByFiles,
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
             maxTriplesByFiles: Int,
             referenceUriPrefix: String,
             packSize : Int,
             apiKey : String,
             timeout : Int,
             verbose: Boolean,
             debug: Boolean) {
    println("============== Main Build ====================")
    println(s"categoryMsd=$categoryMsd,databaseMsd=$databaseMsd,versionMsd=$versionMsd")
    println("==============  getPMIDListFromReference ====================")
    val listReferenceFileNames = MsdUtils(
      rootDir=rootMsdDirectory,
      category=categoryMsd,
      database=databaseMsd,
      spark=spark).getListFiles(versionMsd,".*_type_.*\\.ttl")

    println("================listReferenceFileNames==============")
    println(listReferenceFileNames)

    if (listReferenceFileNames.length<=0) {
      println(s"None reference file in $rootMsdDirectory/$categoryMsd/$databaseMsd/$versionMsd")
      spark.close()
      System.exit(0)
    }

    val pmids = listReferenceFileNames.flatMap(
      referenceFileName => PmidCidWork
      .getPMIDListFromReference(spark,referenceFileName))
    println(s"================PMID List (${pmids.length})==============")
    println(pmids.slice(1,100)+"...")

    val pmidCitoDiscussesCid = EUtils.elink(apikey=apiKey,packSize=packSize,dbFrom="pubmed", db="pccompound",pmids)
    println(s"================pmidCitoDiscussesCid (${pmidCitoDiscussesCid.size})==============")
    println(pmidCitoDiscussesCid.slice(1,100)+"...")

    val turtle : Unit = PmidCidWork.buildCitoDiscusses(pmidCitoDiscussesCid)
    println(s"================ Write Turtle $rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionMsd/pmid_cid.ttl ==============")
    MsdUtils(
      rootDir=rootMsdDirectory,
      category=forumCategoryMsd,
      database=forumDatabaseMsd,
      spark=spark).writeRdf("turtle","TURTLE",versionMsd,"pmid_cid.ttl")

    spark.close()
  }

}
