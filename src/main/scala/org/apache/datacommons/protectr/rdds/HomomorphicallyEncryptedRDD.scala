package org.apache.datacommons.protectr.rdds

import java.math.BigInteger

import com.n1analytics.paillier.{PaillierPrivateKey, EncryptedNumber}
import org.apache.datacommons.protectr.encryptors.EncryptionKeyPair
import org.apache.datacommons.protectr.types.FileType
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

class HomomorphicallyEncryptedRDD
(RDD: RDD[String], keyPair: EncryptionKeyPair, fileType: FileType) extends RDD[String](RDD) {

  def sum(columnIndex: Int): BigInteger = {
    val finalRecord = this.reduce((firstRow, secondRow) => {
      val firstRecord: Array[String] = fileType.parseRecord(firstRow)
      val secondRecord: Array[String] = fileType.parseRecord(secondRow)
      val firstNumber: EncryptedNumber = EncryptedNumber.create(
        firstRecord(columnIndex), keyPair.getPrivateKey)
      val secondNumber: EncryptedNumber = EncryptedNumber.create(
        secondRecord(columnIndex), keyPair.getPrivateKey)
      firstRecord(columnIndex) = firstNumber.add(secondNumber).toString
      fileType.join(firstRecord)
    })
    val sum: String = fileType.parseRecord(finalRecord)(columnIndex)
    val result: EncryptedNumber = EncryptedNumber.create(sum, keyPair.getPrivateKey)
    result.decrypt(keyPair.getPrivateKey).decodeApproximateBigInteger
  }

  def decrypt(columnIndex: Int): UnencryptedRDD = {
    val privateKey: PaillierPrivateKey = keyPair.getPrivateKey
    val javaRDD = this.map(row =>{
        val values: Array[String] = fileType.parseRecord(row)
        val encryptedNumber: EncryptedNumber = EncryptedNumber.create(
          values(columnIndex), keyPair.getPrivateKey)
        val bigInteger: BigInteger = privateKey.decrypt(encryptedNumber).decodeApproximateBigInteger
        values(columnIndex) = bigInteger.toString
        fileType.join(values)
      })
    new UnencryptedRDD(javaRDD,fileType)
  }

  override protected def getPartitions = RDD.partitions

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    RDD.compute(split, context)
  }
}
