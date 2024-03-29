package fr.inrae.msd.rdf

import fr.inrae.msd.rdf.EUtils.ElinkData
import fr.inrae.semantic_web.ProvenanceBuilder
import org.apache.jena.graph.Triple
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import java.text.SimpleDateFormat
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
                     forumCategoryMsd : String = "forum/DiseaseChem",
                     forumDatabaseMsd : String = "PMID_CID",
                     pubchemCategoryMsd : String = "pubchem", //"/rdf/pubchem/compound-general/2021-11-23",
                     pubchemDatabaseMsd : String = "reference", // "/rdf/pubchem/reference/2021-11-23",
                     pubchemVersionMsd: Option[String] = None,
                     implGetPMID: Int = 0, /* 0 : Dataset[Triple], 1 : [RDD[Binding], 2 : Triples.getSubject */
                     packSize : Int = 5000,
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
      opt[Int]('p',"packSize")
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
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.kryo.registrator","net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

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
          config.packSize,
          config.apiKey match {
            case Some(apiK) => apiK
            case None => ""
          })
      case _ =>
        // arguments are bad, error message will have been displayed
        System.err.println("exit with error.")
    }


  def build(
             rootMsdDirectory : String,
             forumCategoryMsd : String,
             forumDatabaseMsd : String,
             categoryMsd : String,
             databaseMsd : String,
             versionMsd: String,
             implGetPMID: Int,
             packSize : Int,
             apiKey : String) : Unit = {

    val startBuild = new Date()

    println("============== Main Build ====================")
    println(s"categoryMsd=$categoryMsd,databaseMsd=$databaseMsd,versionMsd=$versionMsd")
    println("==============  getPMIDListFromReference ====================")
    val listReferenceFileNames = MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category=categoryMsd,
      database=databaseMsd,
      version=Some(versionMsd)).getListFiles(".*_type.*\\.ttl")

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

    println("PATITION BEFORE="+ pmids.partitions.length)
    /* repartition to optimize elink http request */
    val pmidsRep : RDD[String] = pmids.repartition(numPartitions = (numberOfPmid / packSize).toInt + 1) /* repartition to call elink process */
    println("PATITION NEXT="+ pmidsRep.partitions.length)

    import spark.implicits._
    val pmidCitoDiscussesCid : RDD[Either[Seq[ElinkData],Seq[String]]] = EUtils.elink(apikey=apiKey,dbFrom="pubmed", db="pccompound",pmidsRep)
    //println(s"================pmidCitoDiscussesCid (${pmidCitoDiscussesCid.count()})==PARTITION SIZE=${pmidCitoDiscussesCid.partitions.size}============")
/*
    println("=================================")
    pmidCitoDiscussesCid.take(5).foreach(println)
    println("=================================")
*/
    implicit val encoderSchema: Encoder[ElinkData] = Encoders.product[ElinkData]
    implicit val seqStringStringEncoder: Encoder[(String,Seq[String])] = Encoders.product[(String,Seq[String])]

    val pmidCitoDiscussesCidOk : Dataset[(String,Seq[String])] =
      pmidCitoDiscussesCid
        .filter( _.isLeft )
        .flatMap( _.left.get )
        .map( v => v.pmid -> v.cids ).toDS()

    val pmidCitoDiscussesCidKo : Dataset[String] = pmidCitoDiscussesCid.filter( _.isRight ).flatMap(v => v.right.get ).toDS()

    val contributors : Dataset[ElinkData] = pmidCitoDiscussesCid.filter( _.isLeft ).flatMap( _.left.get ).toDS()

    println(" ========== save pmid list without success elink request ========")
    //val lProblemPmid = pmids diff pmidCitoDiscussesCid.keys.toSeq
    println(s"=====================================================")
    println(s"=====================================================")
    println(s"=====================================================")
    //println(" pmid all    :" + pmidCitoDiscussesCid.count())
    //println(" pmid problem:" + pmidCitoDiscussesCidKo.count())
    //println(" pmid OK     :" + pmidCitoDiscussesCidOk.count())
    println(s"================== BUILD VERSION ===================================")
    val formatter = new SimpleDateFormat("yyyy-MM-dd-HHmmss")
    val dateVersion : String = formatter.format(new Date())
    val versionPMIDCID = s"${versionMsd}_$dateVersion"
    println(s"=====================================================")
    println(s"=====================================================")
    println(s"================ Write Turtle $rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionPMIDCID/error_with_pmid ==============")

    MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category=forumCategoryMsd,
      database=forumDatabaseMsd,
      version=Some(versionPMIDCID)).writeDataframeAsTxt(spark,pmidCitoDiscussesCidKo,"error_with_pmid")

    val triples_asso_pmid_cid : Dataset[Triple] = PmidCidWork.buildCitoDiscusses(pmidCitoDiscussesCidOk)
    val triples_contributor : Dataset[Triple] = PmidCidWork.buildContributors(spark,contributors)


    import net.sansa_stack.rdf.spark.io._
    triples_asso_pmid_cid.rdd.saveAsNTriplesFile(s"$rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionPMIDCID/pmid_cid.nt",mode=SaveMode.Overwrite)
    triples_contributor.rdd.saveAsNTriplesFile(s"$rootMsdDirectory/$forumCategoryMsd/$forumDatabaseMsd/$versionPMIDCID/pmid_cid_endpoints.nt",mode=SaveMode.Overwrite)


    val contentProvenanceRDF : String =
      ProvenanceBuilder.provSparkSubmit(
      projectUrl ="https://github.com/p2m2/msd-metdisease-database-pmid-cid-builder",
      category = forumCategoryMsd,
      database = forumDatabaseMsd,
      release=versionPMIDCID,
      startDate = startBuild,
      spark
    )

    MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category="prov",
      database="",
      version=Some("")).writeFile(spark,contentProvenanceRDF,"msd-metdisease-database-pmid-cid-builder-"+versionPMIDCID+".ttl")

    spark.close()
  }

}
