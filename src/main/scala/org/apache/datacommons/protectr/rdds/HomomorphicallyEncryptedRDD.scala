package org.apache.datacommons.protectr.rdds

import java.math.BigInteger

import com.n1analytics.paillier.{EncryptedNumber, PaillierPrivateKey}
import org.apache.datacommons.protectr.encryptors.EncryptionKeyPair
import org.apache.datacommons.protectr.types.FileType
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.util.Try

class HomomorphicallyEncryptedRDD(
                                   RDD: RDD[String],
                                   keyPair: EncryptionKeyPair,
                                   fileType: FileType) extends RDD[String](RDD) {

  private val privateKey: PaillierPrivateKey = keyPair.getPrivateKey

  private def isEncryptedColumn(columnIndex: Int): Boolean = {
    val firstColumnValue = fileType.parseRecord(this.first())(columnIndex)
    Try(EncryptedNumber.create(firstColumnValue, privateKey)).isSuccess
  }


  def sum(columnIndex: Int): BigInteger = {
    if (isEncryptedColumn(columnIndex)) {
      sumEncryptedColumn(columnIndex)
    } else {
      sumNonEncryptedColumn(columnIndex)
    }
  }

  private def sumNonEncryptedColumn(columnIndex: Int): BigInteger = {
    val finalRecord: String = this.reduce((firstRow, secondRow) => {

      val firstRecord: Array[String] = fileType.parseRecord(firstRow)
      val secondRecord: Array[String] = fileType.parseRecord(secondRow)

      val firstNumber = new BigInteger(firstRecord(columnIndex))
      val secondNumber = new BigInteger(secondRecord(columnIndex))

      firstRecord(columnIndex) = firstNumber.add(secondNumber).toString
      fileType.join(firstRecord)
    })

    val sum: String = fileType.parseRecord(finalRecord)(columnIndex)
    new BigInteger(sum)
  }

  private def sumEncryptedColumn(columnIndex: Int): BigInteger = {
    val finalRecord: String = this.reduce((firstRow, secondRow) => {

      val firstRecord: Array[String] = fileType.parseRecord(firstRow)
      val secondRecord: Array[String] = fileType.parseRecord(secondRow)

      val firstNumber: EncryptedNumber = EncryptedNumber.create(
        firstRecord(columnIndex), privateKey)
      val secondNumber: EncryptedNumber = EncryptedNumber.create(
        secondRecord(columnIndex), privateKey)

      firstRecord(columnIndex) = firstNumber.add(secondNumber).toString
      fileType.join(firstRecord)
    })

    val sum: String = fileType.parseRecord(finalRecord)(columnIndex)
    val result: EncryptedNumber = EncryptedNumber.create(sum, privateKey)
    result.decrypt(privateKey).decodeApproximateBigInteger
  }

  def decrypt(columnIndex: Int): UnencryptedRDD = {
    val javaRDD = this.map(row => {
      val values: Array[String] = fileType.parseRecord(row)
      val encryptedNumber: EncryptedNumber = EncryptedNumber.create(
        values(columnIndex), privateKey)
      val bigInteger: BigInteger = privateKey.decrypt(encryptedNumber).decodeApproximateBigInteger
      values(columnIndex) = bigInteger.toString
      fileType.join(values)
    })
    new UnencryptedRDD(javaRDD, fileType)
  }

  override protected def getPartitions: Array[Partition] = RDD.partitions

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    RDD.compute(split, context)
  }
}
