package fr.inrae.msd.rdf

import org.apache.spark.sql.SparkSession


/**
 * https://services.pfem.clermont.inrae.fr/gitlab/forum/metdiseasedatabase/-/blob/develop/app/build/import_PMID_CID.py
 * build/import_PMID_CID.py
 *
 * example using corese rdf4j : https://notes.inria.fr/s/OB038LBLV
 */

case object PmidCidBuilder extends App {

  import scopt.OParser

  case class Config(
                     categoryMsd : String = "pubchem", //"/rdf/pubchem/compound-general/2021-11-23",
                     databaseMsd : String = "reference", // "/rdf/pubchem/reference/2021-11-23",
                     versionMsd: Option[String] = None,
                     maxTriplesByFiles: Int = 5000000,
                     referenceUriPrefix: String = "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID",
                     packSize : Int = 5000,
                     apiKey : Option[String] = None,
                     timeout : Int = 1200,
                     verbose: Boolean = false,
                     debug: Boolean = false)

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("msd-metdisease-database-pmid-cid-builder"),
      head("msd-metdisease-database-pmid-cid-builder", "1.0"),
      opt[String]('r', "versionMsd")
        .required()
        .valueName("<versionMsd>")
        .action((x, c) => c.copy(versionMsd = Some(x)))
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
    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        // do something
        println(config)
        main(
          config.categoryMsd,
          config.databaseMsd,
          config.versionMsd match {
            case Some(version) => version
            case None => MsdUtils(config.categoryMsd,config.databaseMsd,spark).getLastVersion()
          },
          config.maxTriplesByFiles,
          config.referenceUriPrefix,
          config.packSize,
          config.apiKey match {
            case Some(apiK) => apiK
            case None => throw new Exception("None API key pubchem defined.")
          },
          config.timeout,
          config.verbose,
          config.debug)
      case _ =>
        // arguments are bad, error message will have been displayed
        System.err.println("exit with error.")
  }


  def main( categoryMsd : String,
            databaseMsd : String,
            versionMsd: String,
            maxTriplesByFiles: Int,
            referenceUriPrefix: String,
            packSize : Int,
            apiKey : String,
            timeout : Int,
            verbose: Boolean,
            debug: Boolean) {

    PmidCidWork.buildCitoDiscusses(EUtils.elink(
      dbFrom="pubmed",
      db="pccompound",
      PmidCidWork.getPMIDListFromReference(spark,"path/...")))

  }

}
