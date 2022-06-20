package fr.inrae.msd.rdf

import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case object EUtils {
  val base          :String = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
  val durationRetry : Int   = 3000 /* milliseconds */

  def request(apikey: String,
              dbFrom: String,
              db: String,
              uid_list_sub: Seq[String]
             ): Seq[(String, Seq[String])] = {
    val p = requests.post(
      base + s"elink.fcgi",
      params = Seq(
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
  def retry[T](n: Int)(fn: => T): Option[T] = {
    Try { fn } match {
      case Success(x) => Some(x)
      case _ if n > 0 => println(s"***RETRY $n") ; Thread.sleep(durationRetry); retry(n - 1)(fn)
      case _ => None
    }
  }

  /**
   * Finding Related Data Through Entrez Links
   */

  def elink(
             apikey:String,
             packSize : Int =20,
             dbFrom : String,
             db : String,
             uid_list : Seq[String]) : Map[String,Seq[String]] = {

    println("*********************************elink**************************")
      uid_list
      //.slice(1,20)
      .map(_.toLowerCase.split("pmid")(1).trim)
      .grouped(packSize)
      .flatMap(
      uid_list_sub => {
        println("*********************************REQUEST**************************")
        Try(retry(3)(request(apikey,dbFrom,db,uid_list_sub))) match {
          case Success(l) => l
          case Failure(_) => None
        }
        }).toSeq.flatten
  }.toMap

}
