package org.apache.datacommons.protectr.encryptors

import org.apache.datacommons.protectr.types.FileType
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

class HomomorphicallyEncryptedRDD
(RDD: RDD[String], keyPair: EncryptionKeyPair, fileType: FileType) extends RDD[String](RDD) {

  override protected def getPartitions = RDD.partitions

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    RDD.compute(split, context)
  }
}
