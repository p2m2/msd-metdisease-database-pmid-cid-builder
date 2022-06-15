package fr.inrae.msd.rdf

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession

case class MsdUtils(rootDir : String = "/rdf", category : String, database : String,spark : SparkSession) {
  val basedir= s"$rootDir/$category/$database/"

  val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

  def getLastVersion() : String = {
    println("************** getLastVersion ************************** ")
    fs.listStatus(new Path(s"$basedir"))
      .filter(_.isDirectory)
      .map( (a : FileStatus) => (a.getModificationTime,a.getPath) )
      .sortWith( (a,b) => a._1<b._1)
      .lastOption match {
      case Some(value) => value._2.getName
      case None => throw new Exception(s"Can not get last version at $basedir")
    }
  }

  def getListFiles(versionDirectory : String,filterString : String): Seq[String] = {
    fs.listStatus(new Path(s"$basedir/$versionDirectory"))
      .filter( _.getPath.toString.matches(filterString))
      .map( (a : FileStatus) => a.getPath.toString )
  }

  def getPath(version : String ) = s"$basedir/$version"

  def writeRdf(model:String,format : String, version : String, outputPathFile : String): Unit = {

  }

}
