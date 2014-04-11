package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes._

object Columns {
  val ProcessorId = toBytes("id")
  val SequenceNr = toBytes("sn")
  val Marker = toBytes("mk")
  val Message = toBytes("pl")
}