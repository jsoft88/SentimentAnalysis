package org.jc.sparknaivebayes.utils

import org.apache.spark.mllib.linalg.Vectors
import java.io.Serializable

import com.google.common.collect.ImmutableBiMap
import org.apache.spark.rdd.RDD

/**
  * Created by cespedjo on 15/02/2017.
  */
class Dictionary(dict: RDD[String]) extends Serializable {

  //val builder = ImmutableBiMap.builder[String, Long]()
  //dict.zipWithIndex.foreach(e => builder.put(e._1, e._2))

  //val termToIndex = builder.build()
  val termToIndex = dict.zipWithIndex()

  //@transient
  //lazy val indexToTerm = termToIndex.inverse()
  lazy val indexToTerm = dict.zipWithIndex().map{
    case (k, v) => (v, k)
  } //converts from (a, 0),(b, 1),(c, 2) to (0, a),(1, b),(2, c)

  val count = termToIndex.count().toInt

  def indexOf(term: String): Int = termToIndex.lookup(term).headOption.getOrElse[Long](-1).toInt

  def valueOf(index: Int): String = indexToTerm.lookup(index).headOption.getOrElse("")

  def tfIdfs (terms: Seq[String], idfs: Map[String, Double]): Seq[(Int, Double)] = {
    val filteredTerms = terms.filter(idfs contains)
    (filteredTerms.groupBy(identity).map {
      case (term, instances) => {
        val indexOfTerm: Int = indexOf(term)
        if (indexOfTerm < 0) (-1, 0.0) else (indexOf(term), (instances.size.toDouble / filteredTerms.size.toDouble) * idfs(term))
      }
    }).filter(p => p._1.toInt  >= 0).toSeq.sortBy(_._1)
  }

  def vectorize(tfIdfs: Iterable[(Int, Double)]) = {
    Vectors.sparse(dict.count().toInt, tfIdfs.toSeq)
  }
}
