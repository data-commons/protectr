package org.apache.datacommons.protectr.types

abstract class FileType extends Serializable {
  def join(values: Array[String]): String

  def parseRecord(record: String): Array[String]
}

object CSV extends FileType {
  override def join(values: Array[String]): String = values.mkString(",")

  override def parseRecord(record: String): Array[String] = record.split(",", -1)
}

