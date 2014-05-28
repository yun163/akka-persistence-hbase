package akka.persistence.hbase.common

import java.util.{ ArrayList => JArrayList }
import org.apache.hadoop.hbase.util.Bytes._
import org.hbase.async.KeyValue

object Columns {
  val ProcessorId = toBytes("id")
  val SequenceNr = toBytes("seq")
  val Marker = toBytes("mk")
  val Message = toBytes("msg")

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]
}