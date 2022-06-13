package fr.inrae.msd.rdf

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PmidCidBuilderSpec extends AnyFlatSpec with Matchers {
  var spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

/*
  "The Hello object" should "say hello" in {
    PmidCidBuilder.testRun(spark) shouldEqual "hello"
  }
*/
  "Gt PMID List" should "say hello" in {
    PmidCidBuilder.getPMIDListFromReference(
      spark,
      getClass.getResource("/pc_reference_type_test.ttl").getPath).length shouldEqual 497
  }

  "elink" should "say hello" in {
    EUtils.elink("pubmed","pccompound",Seq("7844144","234739")) shouldEqual Seq()
  }
}
