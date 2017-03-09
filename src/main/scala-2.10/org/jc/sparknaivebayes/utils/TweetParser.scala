package org.jc.sparknaivebayes.utils

import org.apache.spark.SparkContext

import scala.io.Source

/**
  * Created by cespedjo on 14/02/2017.
  */
object TweetParser extends Serializable{

  val headerPart = "polarity"

  val mentionRegex = """@(.)+?\s""".r

  val fullRegex = """(\d+),(.+?),(N|P|NEU|NONE)(,\w+|;\w+)*""".r

  def parseAll(csvFiles: Iterable[String], sc: SparkContext) = csvFiles flatMap(csv => parse(csv, sc))

  def parse(csvFile: String, sc: SparkContext) = {
    val csv = sc.textFile(csvFile)
    val docs = scala.collection.mutable.ArrayBuffer.empty[Document]

    csv.foreach(
      line => if (!line.contains(headerPart)) docs += buildDocument(line)
    )
    docs
    //docs.filter(!_.docId.equals("INVALID"))
  }

  def buildDocument(line: String): Document = {

    val lineSplit = line.split(",")
    val id = lineSplit.head
    val txt = lineSplit.tail.init.init.mkString(",")
    val sent = lineSplit.init.last
    val opt = lineSplit.last

    if (id != null && txt != null && sent != null) {
      if (txt.equals("")) {
        //the line does not contain the option after sentiment
        new Document(id, mentionRegex.replaceAllIn(sent, ""), Set(opt))
      } else {
        new Document(id, mentionRegex.replaceAllIn(txt, ""), Set(sent))
      }
    } else {
      println("Invalid")
      new Document("INVALID")
    }
  }
}

case class Document(docId: String, body: String = "", labels: Set[String] = Set.empty)
