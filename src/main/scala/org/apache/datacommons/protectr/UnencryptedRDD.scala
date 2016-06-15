package org.apache.datacommons.protectr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


object UnencryptedRDD {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkJoins").setMaster("local")
    val context = new SparkContext(conf)
    val list: List[Int] = List(1,2,3,4)
    val rdd: RDD[Int] = context.parallelize(list)
    print(rdd.count())
  }
}
