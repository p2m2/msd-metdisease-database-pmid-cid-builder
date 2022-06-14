package fr.inrae.msd.rdf
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

case class MsdUtils(rootDir : String = "/rdf", category : String, database : String,spark : SparkSession) {
  val basedir= s"$rootDir/$category/$database"

  def getLastVersion() : String = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs.listStatus(new Path(s"${basedir}"))
      .filter(_.isDirectory)
      .map( (a : FileStatus) => (a.getModificationTime,a.getPath) )
      .sortWith( (a,b) => a._1<b._1)
      .lastOption match {
      case Some(value) => value._2.getName
      case None => ""
    }
  }

  def getPath(version : String ) = s"$basedir/$version"

}
