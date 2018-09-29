package org.apache.datacommons.protectr.rdds

import java.math.BigInteger

import org.apache.datacommons.protectr.encryptors.EncryptionKeyPair
import org.apache.datacommons.protectr.types.CSV
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class UnencryptedRDDTest extends FunSuite with BeforeAndAfterEach {

  private var sc: SparkContext = _
  private val data = Array("1,23", "2,45", "3,65", "4,67", "5,23")

  override def beforeEach() {
    val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName).setMaster("local")
    sc = new SparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  override def afterEach() {
    sc.stop()
  }

  test("should be able to count on encryptedRdd") {
    val unencryptedRDD: UnencryptedRDD = getUnencryptedRDDOf(data)
    val pair: EncryptionKeyPair = new EncryptionKeyPair(1024)
    val encryptedRDD: HomomorphicallyEncryptedRDD = unencryptedRDD.encryptHomomorphically(pair, 0)
    assert(5 == encryptedRDD.count())
  }

  test("should be able to add an encrypted rdd's column") {
    val unencryptedRDD: UnencryptedRDD = getUnencryptedRDDOf(data)
    val pair: EncryptionKeyPair = new EncryptionKeyPair(1024)
    val encryptedRDD: HomomorphicallyEncryptedRDD = unencryptedRDD.encryptHomomorphically(pair,0)
    val sum: BigInteger = encryptedRDD.sum(0)
    assert(sum==new BigInteger("15"))
  }

  test("should be able to decrypt an RDD") {
    val unencryptedRDD: UnencryptedRDD = getUnencryptedRDDOf(data)
    val pair: EncryptionKeyPair = new EncryptionKeyPair(1024)
    val encryptedRDD: HomomorphicallyEncryptedRDD = unencryptedRDD.encryptHomomorphically(pair, 0)
    val decryptedRDD = encryptedRDD.decrypt(0)
    assert(decryptedRDD.collect() sameElements data)
  }

  test("should be able to add an non-encryped rdd's column") {
    val unencryptedRDD: UnencryptedRDD = getUnencryptedRDDOf(data)
    val pair: EncryptionKeyPair = new EncryptionKeyPair(1024)
    val encryptedRDD: HomomorphicallyEncryptedRDD = unencryptedRDD.encryptHomomorphically(pair,0)
    val sum: BigInteger = encryptedRDD.sum(1)
    assert(sum==new BigInteger("223"))
  }

  private def getUnencryptedRDDOf(data: Array[String]) = {
    val dataSet: RDD[String] = sc.parallelize(data)
    val unencryptedRDD: UnencryptedRDD = new UnencryptedRDD(dataSet, CSV)
    unencryptedRDD
  }
}
