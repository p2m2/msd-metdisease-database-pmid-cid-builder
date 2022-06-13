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

  val r =  Map(
    "7844144" -> Seq(),
    "234739" -> Seq("33558", "23973", "6274","5975","1986","312")
  )

  "elink" should "work" in {
    EUtils.elink("pubmed","pccompound",Seq("7844144","234739")) shouldEqual r
  }

  "buildCitoDiscusses" should "work" in {
    PmidCidBuilder.buildCitoDiscusses(r)
  }
}
