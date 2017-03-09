package org.jc.sparknaivebayes.utils

/**
  * Created by cespedjo on 14/02/2017.
  */
import java.io.StringReader

import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.util.Version
import org.apache.spark.rdd.RDD

object Tokenizer {

  val LuceneVersion = Version.LUCENE_5_1_0

  def tokenizeAll(docs: Iterable[Document]) = docs.map(tokenize)

  def tokenize(doc: Document): TermDoc = TermDoc(doc.docId, doc.labels, tokenize(doc.body))

  def tokenize(content: String): Seq[String] = {
    val result = scala.collection.mutable.ArrayBuffer.empty[String]
    /*content.split("\n").foreach(line => line.split(" ").foreach(
      word => if (word.startsWith("#")) result += word.substring(1) else word
    ))*/
    val analyzer = new SpanishAnalyzer()
    analyzer.setVersion(LuceneVersion)
    val tReader = new StringReader(content)
    val tStream = analyzer.tokenStream("", tReader)
    val term = tStream.addAttribute(classOf[CharTermAttribute])

    tStream.reset()
    while (tStream.incrementToken()) {
      val termValue = term.toString
      if (termValue.startsWith("#")) {
        result += termValue.substring(1)
      }
      else {
        result += termValue
      }
    }

    result
  }
}

case class TermDoc(doc: String, labels: Set[String], terms: Seq[String])
