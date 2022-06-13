package fr.inrae.msd.rdf

import org.apache.spark.sql.SparkSession
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.eclipse.rdf4j.rio.{RDFFormat, RDFHandlerException, RDFParseException, Rio}
import org.eclipse.rdf4j.common.exception.RDF4JException
import org.eclipse.rdf4j.query.{GraphQueryResult, QueryResults}

import java.io.{ByteArrayInputStream, IOException, InputStream}

/**
 * https://services.pfem.clermont.inrae.fr/gitlab/forum/metdiseasedatabase/-/blob/develop/app/build/import_PMID_CID.py
 * build/import_PMID_CID.py
 *
 * example using corese rdf4j : https://notes.inria.fr/s/OB038LBLV
 */

object PmidCidBuilder {

  val compound_path = "/rdf/pubchem/compound-general/2021-11-23"
  val reference_path = "/rdf/pubchem/reference/2021-11-23"
  val max_triples_by_files = 5000000
  val reference_uri_prefix = "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID"
  val version = 2021
  val run_as_test = false
  val pack_size = 5000
  val api_key = "30bc501ba6ab4cba2feedffb726cbe825c0a"
  val timeout = 1200

  def getPMIDListFromReference(spark : SparkSession,referencePath: String): Seq[String] = {
    import spark.implicits._
    val rdfParser = Rio.createParser(RDFFormat.TURTLE)

    val model = new LinkedHashModel
    rdfParser.setRDFHandler(new StatementCollector(model))

    val targetStream : InputStream  =
      new ByteArrayInputStream(
        spark.read.text(referencePath).toDF("lineRdf").collect().mkString("\n").getBytes())

    println(spark.read.text(referencePath).toDF("lineRdf").collect().mkString("\n"))


    try {
      val baseUri : java.lang.String = ""
      val res : GraphQueryResult = QueryResults.parseGraphBackground(targetStream, baseUri, RDFFormat.TURTLE)
      try while ( {
        res.hasNext
      }) {
        val st = res.next
        // ... do something with the resulting statement here.
      }
      catch {
        case e: RDF4JException =>

        // handle unrecoverable error
      } finally {
        inputStream.close
        if (res != null) res.close()
      }
    }

    try {
      val results : Model = Rio.parse(targetStream, referencePath, RDFFormat.TURTLE)
      println("===================================================WWWWWWWWWWWWWWWOKKK")
    } catch {
      case e: IOException =>
        System.err.println(" ==================== IOException ===================")
        System.err.println(e.getMessage())
      // handle IO problems (e.g. the file could not be read)
      case e: RDFParseException =>
        System.err.println(" ==================== RDFParseException ===================")
        System.err.println(e.getMessage())
      // handle unrecoverable parse error
      case e: RDFHandlerException =>
        System.err.println(" ==================== RDFHandlerException ===================")
        System.err.println(e.getMessage())
        System.err.println(e.getMessage())
      // handle a problem encountered by the RDFHandler
    } finally targetStream.close
    Seq()
  }

  def buildCitoDiscusses(mapPmidCid : Map[String,Seq[String]]) = {
    println("@prefix cito: <http://purl.org/spar/cito/> .")
    println("@prefix compound: <http://rdf.ncbi.nlm.nih.gov/pubchem/compound/> .")
    println("@prefix reference: <http://rdf.ncbi.nlm.nih.gov/pubchem/reference/> .")

    mapPmidCid.map {
      case (pmid,listCid) =>listCid.foreach( cid =>
        println(s"reference:PMID$pmid cito:discusses compound:CID$cid") )
    }
  }

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("msd-metdisease-database-pmid-cid-builder")
      .getOrCreate()

    buildCitoDiscusses(EUtils.elink(
      dbFrom="pubmed",
      db="pccompound",
      getPMIDListFromReference(spark,"path/...")))
  }
}
