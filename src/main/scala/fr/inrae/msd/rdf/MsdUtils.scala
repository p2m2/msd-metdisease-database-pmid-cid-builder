package fr.inrae.msd.rdf

import org.apache.hadoop.fs._
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class MsdUtils(
                     rootDir : String = "/rdf",
                     spark : SparkSession,
                     category : String,
                     database : String,
                     version : String = ""
                     ) {

  val basedir : String     = s"$rootDir/$category/$database/"
  val fs: FileSystem       = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val versionUsed : String = version match {
    case v if v.isEmpty => getLastVersion
    case _ => version
  }

  def getLastVersion: String = {
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

  def getListFiles(filterString : String): Seq[String] = {
    fs.listStatus(new Path(s"$basedir/$versionUsed"))
      .filter( _.getPath.toString.matches(filterString))
      .map( (a : FileStatus) => a.getPath.toString )
  }

  def getPath : String = s"$basedir/$versionUsed"

  def writeRdf(model:Model, format : Lang, outputPathFile : String): Unit = {
    val outDir : String = basedir+"/"+versionUsed

    if (! fs.exists(new Path(outDir))) {
      fs.mkdirs(new Path(outDir))
    }

    val path = new Path(s"$outDir/$outputPathFile")
    val out : FSDataOutputStream = FileSystem.create(fs,path,FileContext.DEFAULT_PERM)
    try RDFDataMgr.write(out, model, format)
    finally out.close
  }

  def writeDataframeAsTxt(spark: SparkSession , contain:RDD[String], outputPathFile : String): Unit = {
    import spark.implicits._

    val outDir : String = basedir+"/"+versionUsed

    if (! fs.exists(new Path(outDir))) {
      fs.mkdirs(new Path(outDir))
    }

    contain
      .toDF()
      .write
      .mode("overwrite")
      .text(s"$outDir/$outputPathFile")
  }

  def writeFile(spark: SparkSession , content: String, outputPathFile : String): Unit = {
    import spark.implicits._

    val outDir: String = basedir + "/" + versionUsed

    if (!fs.exists(new Path(outDir))) {
      fs.mkdirs(new Path(outDir))
    }

    val hdfsWritePath = new Path(outDir + "/" + outputPathFile)

    val outputStream = fs.create(hdfsWritePath)
    outputStream.writeBytes(content)
    outputStream.close()
  }
}
