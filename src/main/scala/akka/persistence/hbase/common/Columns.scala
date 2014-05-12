package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes._

object Columns {
  val ProcessorId = toBytes("id")
  val SequenceNr = toBytes("seq")
  val Marker = toBytes("mk")
  val Message = toBytes("msg")
}