package org.apache.datacommons.protectr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{FunSuite, ShouldMatchers}


class UnencryptedRDDTest extends FunSuite with ShouldMatchers {
  test ("should be able to count on unencryptedRdd"){
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc: SparkContext = new SparkContext(conf)
    val data = Array("1", "2", "3", "4", "5")
    val dataSet: RDD[String] = sc.parallelize(data)
    val unencryptedRDD: UnencryptedRDD = new UnencryptedRDD(dataSet)
    assert(5==unencryptedRDD.count())
  }
}
