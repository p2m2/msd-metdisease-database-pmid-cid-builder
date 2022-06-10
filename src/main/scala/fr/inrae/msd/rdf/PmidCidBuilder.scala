package fr.inrae.msd.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * https://services.pfem.clermont.inrae.fr/gitlab/forum/metdiseasedatabase/-/blob/develop/app/build/import_PMID_CID.py
 * build/import_PMID_CID.py
 */

object PmidCidBuilder {

  val compound_path = "/rdf/pubchem/compound-general/2021-11-23"
  val reference_path = "/rdf/pubchem/reference/2021-11-23"
  val max_triples_by_files = 5000000
  val reference_uri_prefix = "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID"
  val version = 2021
  val run_as_test = false
  val pack_size = 5000
  val api_key = "30bc501ba6ab4cba2feedffb726cbe825c0a"
  val timeout = 1200

  def getPMIDListFromReference(spark : SparkSession,referencePath: String): Seq[String] = {
    import spark.implicits._

    spark.read.text(referencePath).toDF("lineRdf")
      .filter(
        col("lineRdf").rlike("reference:PMID")
      ).map(
        row => row.toString().split("\\s")(0)
      )
      .collect()
  }

  def testRun(spark : SparkSession): String = {
    import spark.implicits._
    spark.range(0,1000).map( _ * 2 ).show()
    "hello"
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("msd-metdisease-database-pmid-cid-builder")
      .getOrCreate()

    testRun(spark)
  }
}
