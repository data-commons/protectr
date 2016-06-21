package org.apache.datacommons.protectr.rdds

import java.math.BigInteger

import org.apache.datacommons.protectr.encryptors.EncryptionKeyPair
import org.apache.datacommons.protectr.types.CSV
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class UnencryptedRDDTest extends FunSuite with BeforeAndAfterEach{

  var sc :SparkContext = null
  override def beforeEach() {
    val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  override def afterEach() {
    sc.stop()
  }

  test ("should be able to count on encryptedRdd"){
    val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
    val dataSet: RDD[String] = sc.parallelize(data)
    val unencryptedRDD: UnencryptedRDD = new UnencryptedRDD(dataSet,CSV)
    val pair: EncryptionKeyPair = new EncryptionKeyPair(1024)
    val encryptedRDD: HomomorphicallyEncryptedRDD = unencryptedRDD.encryptHomomorphically(pair,0)
    assert(5==encryptedRDD.count())
  }

  test("should be able to add an encrypted rdd's column"){
    val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")
    val dataSet: RDD[String] = sc.parallelize(data)
    val unencryptedRDD: UnencryptedRDD = new UnencryptedRDD(dataSet,CSV)
    val pair: EncryptionKeyPair = new EncryptionKeyPair(1024)
    val encryptedRDD: HomomorphicallyEncryptedRDD = unencryptedRDD.encryptHomomorphically(pair,0)
    val sum: BigInteger = encryptedRDD.sum(0)
    assert(sum==new BigInteger("15"))
  }
}
