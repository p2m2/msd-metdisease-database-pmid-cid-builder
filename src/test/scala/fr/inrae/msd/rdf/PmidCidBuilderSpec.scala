package fr.inrae.msd.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PmidCidBuilderSpec extends AnyFlatSpec with Matchers {
  var spark =
    SparkSession
    .builder()
      .appName("local-test")
      .master("local[*]")
      .getOrCreate()
/*
  "The Hello object" should "say hello" in {
    PmidCidBuilder.testRun(spark) shouldEqual "hello"
  }
*/
  "Gt PMID List" should "say hello" in {
    PmidCidWork.getPMIDListFromReference_impl1(
      spark,
      getClass.getResource("/pc_reference_type_test.ttl").getPath).count() shouldEqual 497
  }

  val r: Map[String, Seq[String]] =  Map(
    "7844144" -> Seq(),
    "234739" -> Seq("33558", "23973", "6274","5975","1986","312")
  )

  val r2: RDD[String] = spark.sparkContext.parallelize(Seq("7844144","234739"))

  "elink" should "work" in {
    EUtils.elink("aa","pubmed","pccompound",r2).collect() shouldEqual r
  }
  val r3: RDD[(String, Seq[String])] = spark.sparkContext.parallelize(r.toSeq)

  "buildCitoDiscusses" should "work" in {
    PmidCidWork.buildCitoDiscusses(r3)
  }
  "getLastVersion()" should "ok" in {
    MsdUtils(".","src","test",spark).getLastVersion()
  }
}
