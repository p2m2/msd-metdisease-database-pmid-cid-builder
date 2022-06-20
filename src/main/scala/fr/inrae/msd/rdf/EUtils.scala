package fr.inrae.msd.rdf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

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
    println("jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj")
    println(uid_list_sub.mkString(","))
    println("jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj")

    val p = requests.post(
      base + s"elink.fcgi",
      params = Seq(
        "api_key" -> apikey,
        "dbfrom" -> dbFrom,
        "db" -> db) ++ uid_list_sub.map("id" -> _)
    )
    val xml = scala.xml.XML.loadString(p.text)
    println(xml.toString())
    xml \\ "LinkSet" map { linkSet =>
      (linkSet \\ "IdList" \\ "Id").text -> (linkSet \\ "LinkSetDb" \\ "Link" \\ "Id" map { id => id.text })
    }
  }

  // Returning a Try[T] wrapper
  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => println("OKKK");x
      case Failure(e) if n > 0 => {
        println(e.getMessage())
        println(s"***RETRY $n")
        Thread.sleep(durationRetry)
        retry(n - 1)(fn)
      }
      case Failure(e) => println(e.getMessage()) ; throw new Exception("stop")
    }
  }

  /**
   * Finding Related Data Through Entrez Links
   */

  def elink(
             apikey:String,
             dbFrom : String,
             db : String,
             uid_list : RDD[String]) : RDD[(String,Seq[String])] = {

    println("*********************************elink**************************")
      uid_list
        .map(_.toLowerCase.split("pmid")(1).trim)
        .glom()
        .flatMap(listPmids => {
          println("*********************************REQUEST**************************")
          println(listPmids.mkString("*"))
          println("*************************")
          val r :Seq[(String, Seq[String])] = retry(3)(request(apikey,dbFrom,db,listPmids))
          r
        })

  }

}
