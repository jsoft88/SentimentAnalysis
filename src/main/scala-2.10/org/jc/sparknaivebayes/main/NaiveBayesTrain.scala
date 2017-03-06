package org.jc.sparknaivebayes.main

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.jc.sparknaivebayes.utils._

/**
  * Created by cespedjo on 15/02/2017.
  */
object NaiveBayesTrain extends App {

  val sc = new SparkContext()
  sc.setJobDescription("NaiveBayesSentimentAnalysis")

  if (args.length < 2) {
    println("Insufficient args")
  } else {
    val csvFiles = args(0).split(",")
    val modelStore = args(1)
    val docs = TweetParser.parseAll(csvFiles, sc)
    val termDocs = Tokenizer.tokenizeAll(docs)

    val termDocsRdd = sc.parallelize[TermDoc](termDocs.toSeq)

    val numDocs = termDocsRdd.count()

    //val terms = termDocsRdd.flatMap(_.terms).distinct().collect().sortBy(identity)
    val terms = termDocsRdd.flatMap(_.terms).distinct().sortBy(identity)
    val termDict = new Dictionary(terms)

    //val labels = termDocsRdd.flatMap(_.labels).distinct().collect()
    val labels = termDocsRdd.flatMap(_.labels).distinct()
    val labelDict = new Dictionary(labels)

    val idfs = (termDocsRdd.flatMap(termDoc => termDoc.terms.map((termDoc.doc, _))).distinct().groupBy(_._2) collect {
      case (term, docs) if docs.size > 3 =>
        term -> (numDocs.toDouble / docs.size.toDouble)
    }).collect.toMap

    val tfidfs = termDocsRdd flatMap {
      termDoc =>
        val termPairs: Seq[(Int, Double)] = termDict.tfIdfs(termDoc.terms, idfs)
        termDoc.labels.headOption.map {
          label =>
            val labelId = labelDict.indexOf(label).toDouble
            val vector = Vectors.sparse(termDict.count.toInt, termPairs)
            LabeledPoint(labelId, vector)
        }
    }

    val model = NaiveBayes.train(tfidfs)
    NaiveBayesAndDictionaries(model, termDict, labelDict, idfs)
    model.save(sc, modelStore)
    sc.stop()
  }

  case class NaiveBayesAndDictionaries(model: NaiveBayesModel, termDictionary: Dictionary, labelDictionary: Dictionary, idfs: Map[String, Double])
}
