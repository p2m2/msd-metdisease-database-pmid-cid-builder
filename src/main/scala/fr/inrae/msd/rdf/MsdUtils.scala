package fr.inrae.msd.rdf

import org.apache.hadoop.fs.{FSDataOutputStream, FileContext, FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.Lang
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.rdd.RDD

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

  def writeRdf(model:Model, format : Lang, version : String, outputPathFile : String): Unit = {
    val outDir : String = basedir+"/"+version

    if (! fs.exists(new Path(outDir))) {
      fs.mkdirs(new Path(outDir))
    }

    val path = new Path(s"$outDir/$outputPathFile")
    val out : FSDataOutputStream = FileSystem.create(fs,path,FileContext.DEFAULT_PERM)
    try RDFDataMgr.write(out, model, format)
    finally out.close
  }

  def writeDataframeAsTxt(spark: SparkSession , contain:RDD[String], version : String, outputPathFile : String) = {
    import spark.implicits._

    val outDir : String = basedir+"/"+version

    if (! fs.exists(new Path(outDir))) {
      fs.mkdirs(new Path(outDir))
    }

    contain
      .toDF()
      .write
      .mode("overwrite")
      .text(s"$outDir/$outputPathFile")
  }
}
