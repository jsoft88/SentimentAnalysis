package org.jc.sparknaivebayes.main

import org.apache.spark.{SparkConf, SparkContext}
import org.jc.sparknaivebayes.utils.TweetParser

/**
  * Created by cespedjo on 03/03/2017.
  */
object LocalTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("readFiles").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val docs = TweetParser.parseAll(List("/home/cloudera/Downloads/general_train.csv"), sc)
    val count = docs.count()
    println(count)
  }

}
