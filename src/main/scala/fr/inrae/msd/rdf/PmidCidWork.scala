package fr.inrae.msd.rdf

import org.apache.spark.sql.SparkSession
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.eclipse.rdf4j.rio.{RDFFormat, RDFHandlerException, RDFParseException, Rio}

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.nio.charset.StandardCharsets
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

case object PmidCidWork {
  def getPMIDListFromReference(spark : SparkSession,referencePath: String): Seq[String] = {
    println("========= getPMIDListFromReference ===============")
    import spark.implicits._
    println("========= build parser ===============")
    val rdfParser = Rio.createParser(RDFFormat.TURTLE)

    val model = new LinkedHashModel
    rdfParser.setRDFHandler(new StatementCollector(model))
    println(s"========= reading spark referenceFile=$referencePath ===============")
    val str : String = spark.read.text(referencePath).collect().map(row => row.mkString("")).mkString("\n")
    println("========= results(1,1000) ===============")
    println(str.substring(1,1000))


    val targetStream : InputStream  =
      new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8.name()))

    try {
      val results : Model = Rio.parse(targetStream, referencePath, RDFFormat.TURTLE)
      val f = results.getStatements(null,RDF.TYPE,iri("http://purl.org/spar/fabio/JournalArticle"))
      f.toSeq.map( r => r.getSubject.toString.split("http://rdf.ncbi.nlm.nih.gov/pubchem/reference/")(1))
    } catch {
      case e: IOException =>
        System.err.println(" ==================== IOException ===================")
        System.err.println(e.getMessage())
        Seq()
      // handle IO problems (e.g. the file could not be read)
      case e: RDFParseException =>
        System.err.println(" ==================== RDFParseException ===================")
        System.err.println(e.getMessage())
        Seq()
      // handle unrecoverable parse error
      case e: RDFHandlerException =>
        System.err.println(" ==================== RDFHandlerException ===================")
        System.err.println(e.getMessage())
        Seq()
      // handle a problem encountered by the RDFHandler
    } finally targetStream.close
  }

  def buildCitoDiscusses(mapPmidCid : Map[String,Seq[String]])  = {
    println("@prefix cito: <http://purl.org/spar/cito/> .")
    println("@prefix compound: <http://rdf.ncbi.nlm.nih.gov/pubchem/compound/> .")
    println("@prefix reference: <http://rdf.ncbi.nlm.nih.gov/pubchem/reference/> .")

    mapPmidCid.map {
      case (pmid,listCid) =>listCid.foreach( cid =>
        println(s"reference:PMID$pmid cito:discusses compound:CID$cid") )
    }
  }
}
