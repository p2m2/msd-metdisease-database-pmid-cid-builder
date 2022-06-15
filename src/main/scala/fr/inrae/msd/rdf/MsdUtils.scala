package fr.inrae.msd.rdf

import org.apache.hadoop.fs.{FSDataOutputStream, FileContext, FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

case class MsdUtils(rootDir : String = "/rdf", category : String, database : String,spark : SparkSession) {
  val basedir= s"$rootDir/$category/$database/"

  val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

  def getLastVersion() : String = {

    fs.listStatus(new Path(s"$basedir"))
      .filter(_.isDirectory)
      .map( (a : FileStatus) => (a.getModificationTime,a.getPath) )
      .sortWith( (a,b) => a._1<b._1)
      .lastOption match {
      case Some(value) => value._2.getName
      case None => ""
    }
  }

  def getListFiles(versionDirectory : String,filterString : String): Seq[String] = {
    fs.listStatus(new Path(s"$basedir/$versionDirectory"))
      .filter( _.getPath.toString.matches(filterString))
      .map( (a : FileStatus) => a.getPath.toString )
  }

  def getPath(version : String ) = s"$basedir/$version"

  def writeRdf(model:Model,format : RDFFormat, outputPathFile : String): Unit = {

    if (! fs.exists(new Path(basedir))) {
      fs.mkdirs(new Path(basedir))
    }

    val path = new Path(s"$basedir/$outputPathFile")
    val out : FSDataOutputStream = FileSystem.create(fs,path,FileContext.DEFAULT_PERM)
    try Rio.write(model, out, format)
    finally out.close
  }

}
