package org.apache.datacommons.protectr.rdds

import org.apache.datacommons.protectr.encryptors.{HomomorphicallyEncryptedRDD, EncryptionKeyPair}
import org.apache.datacommons.protectr.types.CSV
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FunSuite, ShouldMatchers}


class UnencryptedRDDTest extends FunSuite with ShouldMatchers {

  test ("should be able to count on encryptedRdd"){
    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc: SparkContext = new SparkContext(conf)
    val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
    val dataSet: RDD[String] = sc.parallelize(data)
    val unencryptedRDD: UnencryptedRDD = new UnencryptedRDD(dataSet,CSV)
    val pair: EncryptionKeyPair = new EncryptionKeyPair(1024)
    val encryptedRDD: HomomorphicallyEncryptedRDD = unencryptedRDD.encryptHomomorphically(pair,0)
    assert(5==encryptedRDD.count())
  }
}
