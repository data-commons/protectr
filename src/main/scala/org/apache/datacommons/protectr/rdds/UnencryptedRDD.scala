package org.apache.datacommons.protectr.rdds

import com.n1analytics.paillier.PaillierContext
import com.n1analytics.paillier.PaillierPublicKey
import org.apache.datacommons.protectr.encryptors.EncryptionKeyPair
import org.apache.datacommons.protectr.types.{CSV, FileType}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}


class UnencryptedRDD(parent: RDD[String],fileType: FileType = CSV)
  extends RDD[String](parent) {

  def encryptHomomorphically(keyPair: EncryptionKeyPair, columnIndex: Int)
  : HomomorphicallyEncryptedRDD = {
    val publicKey: PaillierPublicKey = keyPair.getPublicKey
    val signedContext: PaillierContext = publicKey.createSignedContext
    val encryptedRDD = this.map(row => {
      val values: Array[String] = fileType.parseRecord(row)
      val numericValue: String = values(columnIndex)
      values(columnIndex) = signedContext.encrypt(numericValue.toDouble).toString
      fileType.join(values)
    })
    new HomomorphicallyEncryptedRDD(encryptedRDD, keyPair, fileType)
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    parent.compute(split, context)
  }

  override protected def getPartitions: Array[Partition] = parent.partitions
}

