package fr.inrae.msd.rdf

import scala.language.postfixOps

case object EUtils {
  val base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

  /**
   * Finding Related Data Through Entrez Links
   */

  def elink(
             apikey:String,
             packSize : Int =20,
             dbFrom : String,
             db : String,
             uid_list : Seq[String]) : Map[String,Seq[String]] = {

    uid_list
      .map(_.toLowerCase.replace("pmid","").trim)
      .grouped(packSize)
      .flatMap(
      uid_list_sub => {
        println("-----------------------------------")
        println(uid_list_sub)
        val p =requests.post(
          base+s"elink.fcgi",
          params = Seq(
            "api_key" -> apikey,
            "dbfrom" -> dbFrom,
            "db" -> db) ++ uid_list_sub.map( "id" -> _ )
        )
        println("-----------------------------------2")
        println(p.toString)

        val xml = scala.xml.XML.loadString(p.text)

        println("OK====>"+xml)
        xml \\ "LinkSet" map  { linkSet =>
          (linkSet \\ "IdList" \\ "Id").text -> (linkSet \\ "LinkSetDb" \\ "Link" \\ "Id" map { id => id.text }) }
      }.toList)
  }.toMap

}
