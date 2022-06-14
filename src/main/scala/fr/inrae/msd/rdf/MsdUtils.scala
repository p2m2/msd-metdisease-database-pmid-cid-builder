package fr.inrae.msd.rdf
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.SparkSession

case class MsdUtils(category : String, database : String,spark : SparkSession) {
  val root : String = "/rdf"
  val basedir= s"$root/$category/$database"

  def getLastVersion() : String = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    println(" ============================================= getLastVersion =========================")
    fs.listStatus(new Path(s"${basedir}")).filter(_.isDirectory).map(_.getPath).foreach(println)
    ""
  }

  def getPath(version : String ) = s"$basedir/$version"

}
