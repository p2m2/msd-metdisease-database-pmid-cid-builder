package fr.inrae.msd.rdf

import scala.language.postfixOps

case object EUtils {
  val base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

  /**
   * Finding Related Data Through Entrez Links
   */

  def elink(dbFrom : String,db : String, uid_list : Seq[String]) : Map[String,Seq[String]] = {

    println(base+s"elink.fcgi?dbfrom=$dbFrom&db=$db" +
      s"&id=${uid_list.map(_.trim).mkString("&id=")}")
    val xml = scala.xml.XML.loadString(
      requests.get(
        base+s"elink.fcgi?dbfrom=$dbFrom&db=$db" +
          s"&id=${uid_list.map(_.trim).mkString("&id=")}").text)

    xml \\ "LinkSet" map  { linkSet =>
      (linkSet \\ "IdList" \\ "Id").text -> (linkSet \\ "LinkSetDb" \\ "Link" \\ "Id" map { id => id.text }) }
  }.toMap
}
