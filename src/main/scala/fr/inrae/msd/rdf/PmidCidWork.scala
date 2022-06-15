package fr.inrae.msd.rdf

import net.sansa_stack.rdf.spark.io.RDFReader

import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

case object PmidCidWork {
  def getPMIDListFromReference(spark : SparkSession,referencePath: String): Seq[String] = {
    val lang = Lang.TURTLE

    val triples = spark.rdf(lang)(referencePath).collect()
    println(triples.mkString(","))

    val str : String = spark.read.text(referencePath).collect().map(row => row.mkString("")).mkString("\n")
    Seq()
  }

  def buildCitoDiscusses(mapPmidCid : Map[String,Seq[String]]) : Unit  = {

  }
}
