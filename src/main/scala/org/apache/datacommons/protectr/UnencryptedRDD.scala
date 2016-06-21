package org.apache.datacommons.protectr

import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD



class UnencryptedRDD(parent: RDD[String]) extends RDD[String](parent) {
  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    parent.compute(split, context)
  }

  override protected def getPartitions: Array[Partition] = parent.partitions
}

