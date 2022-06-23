package fr.inrae.msd.rdf

import fr.inrae.semantic_web.ProvenanceBuilder
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.Date

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
object PmidCidBuilder extends App {

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
                     packSize : Int = 350,
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
          if ((x >= 0) && (x <=3)) success
          else failure("Value <implementation> must be >0"))
        .valueName("<implGetPMID>")
        .text("implementation to get PMID subject from reference - 0 : Dataset[Triple], 1 : [RDD[Binding], 2 : Triples.getSubject. 3: SparqlFrame"),
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
              spark=spark,
              category=config.pubchemCategoryMsd,
              database=config.pubchemDatabaseMsd).getLastVersion
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


  /**
   * First execution of the work.
   * Build asso PMID <-> CID and a list f PMID error
   * @param rootMsdDirectory
   * @param forumCategoryMsd
   * @param forumDatabaseMsd
   * @param categoryMsd
   * @param databaseMsd
   * @param versionMsd
   * @param implGetPMID
   * @param referenceUriPrefix
   * @param packSize
   * @param apiKey
   * @param timeout
   * @param verbose
   * @param debug
   */
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

    val startBuild = new Date()

    println("============== Main Build ====================")
    println(s"categoryMsd=$categoryMsd,databaseMsd=$databaseMsd,versionMsd=$versionMsd")
    println("==============  getPMIDListFromReference ====================")
    val listReferenceFileNames = MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category=categoryMsd,
      database=databaseMsd,
      version=versionMsd).getListFiles(".*_type.*\\.ttl")

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
        case 3 => PmidCidWork.getPMIDListFromReference_impl4(spark,referenceFileName)
        case _ => PmidCidWork.getPMIDListFromReference_impl1(spark,referenceFileName)
      }))
      val numberOfPmid = pmids.count()

    println("PATITION BEFORE="+ pmids.partitions.size)
    /* repartition to optimize elink http request */
    val pmidsRep = pmids.repartition(numPartitions = (numberOfPmid / packSize).toInt + 1) /* repartition to call elink process */
    println("PATITION NEXT="+ pmidsRep.partitions.size)

    println(s"================PMID List ($numberOfPmid)==============")
    println(pmids.take(10).slice(1,10)+"...")

    val pmidCitoDiscussesCid : RDD[(String,Option[Seq[String]])] = EUtils.elink(apikey=apiKey,dbFrom="pubmed", db="pccompound",pmidsRep)
    println(s"================pmidCitoDiscussesCid (${pmidCitoDiscussesCid.count()})==PARTITION SIZE=${pmidCitoDiscussesCid.partitions.size}============")

    val pmidCitoDiscussesCidOk : RDD[(String,Seq[String])] = pmidCitoDiscussesCid.filter( _._2.isDefined ).map(v => v._1 -> v._2.get)
    val pmidCitoDiscussesCidKo : RDD[String] = pmidCitoDiscussesCid.filter( _._2.isEmpty ).map(v => v._1 )

    println(" ========== save pmid list without success elink request ========")
    //val lProblemPmid = pmids diff pmidCitoDiscussesCid.keys.toSeq
    println(s"=====================================================")
    println(s"=====================================================")
    println(s"=====================================================")
    println(" pmid all    :" + pmidCitoDiscussesCid.count())
    println(" pmid problem:" + pmidCitoDiscussesCidKo.count())
    println(" pmid OK     :" + pmidCitoDiscussesCidOk.count())
    println(s"=====================================================")
    println(s"=====================================================")
    println(s"=====================================================")
    println(s"================ Write Turtle $rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionMsd/error_with_pmid ==============")

    MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category=forumCategoryMsd,
      database=forumDatabaseMsd,
      version=versionMsd).writeDataframeAsTxt(spark,pmidCitoDiscussesCidKo,"error_with_pmid")

    val triples_asso_pmid_cid : RDD[Triple] = PmidCidWork.buildCitoDiscusses(pmidCitoDiscussesCidOk)

    import net.sansa_stack.rdf.spark.io._
    triples_asso_pmid_cid.saveAsNTriplesFile(s"$rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionMsd/pmid_cid.ttl",mode=SaveMode.Overwrite) //.take(5))

    val contentProvenanceRDF : String =
      ProvenanceBuilder.provSparkSubmit(
      projectUrl ="https://github.com/p2m2/msd-metdisease-database-pmid-cid-builder",
      category = forumCategoryMsd,
      database = forumDatabaseMsd,
      release=versionMsd,
      startDate = startBuild,
      spark
    )

    MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category="prov",
      database=forumDatabaseMsd,
      version=versionMsd).writeFile(spark,contentProvenanceRDF,"msd-metdisease-database-pmid-cid-builder-"+versionMsd+".ttl")

    spark.close()
  }

}
