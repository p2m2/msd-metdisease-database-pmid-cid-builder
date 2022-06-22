package fr.inrae.msd.rdf

import org.apache.spark.rdd.RDD

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case object EUtils {
  val base          :String = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
  val durationRetry : Int   = 10000 /* milliseconds */
  val retryNum      : Int   = 10

  def request(apikey: String,
              dbFrom: String,
              db: String,
              uid_list_sub: Seq[String]
             ): Seq[(String, Seq[String])] = {

    val p = requests.post(
      base + s"elink.fcgi",
      compress = requests.Compress.None,
      data = Seq(
        "api_key" -> apikey,
        "dbfrom" -> dbFrom,
        "db" -> db) ++ uid_list_sub.map("id" -> _)
    )
    val xml = scala.xml.XML.loadString(p.text)
    xml \\ "LinkSet" map { linkSet =>
      (linkSet \\ "IdList" \\ "Id").text -> (linkSet \\ "LinkSetDb" \\ "Link" \\ "Id" map { id => id.text })
    }
  }

  // Returning a Try[T] wrapper
  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case Failure(e) if n > 0 => {
        println(e.getMessage())
        println(s"***RETRY $n")
        Thread.sleep(durationRetry)
        retry(n - 1)(fn)
      }
      case Failure(e) => println(e.getMessage()) ; throw new Exception("stop retry")
    }
  }

  /**
   * Finding Related Data Through Entrez Links
   *
   * get PMID -> Some(List(CID)) or None
   */

  def elink(
             apikey:String,
             dbFrom : String,
             db : String,
             uid_list : RDD[String]) : RDD[(String,Option[Seq[String]])] = {

    println("*********************************elink**************************")
     val g= uid_list
         .map(_.toLowerCase.split("pmid")(1).trim)
        .glom()
    println(s"COUNT GLOM=${g.count()}")
       g
        .flatMap(listPmids => {
          println("*********************************REQUEST**************************")
          println(listPmids.length)
          Try(retry(retryNum)(request(apikey,dbFrom,db,listPmids))
            .map(pmid => pmid._1 -> Some(pmid._2))) match {
            case Success(v) => println("v:"+v.mkString(","));v
            case Failure(_) => listPmids.map( pmid => (pmid -> None ) )
          }
        })
  }

}
