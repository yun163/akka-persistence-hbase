package akka.persistence.hbase.common

import com.twitter.util.Eval
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import java.nio.ByteBuffer
import akka.actor.{ Actor, ActorSystem }
import akka.persistence.{ Persistent, PersistentRepr }
import akka.serialization.{ SerializationExtension, Serialization }
import akka.persistence.hbase.journal.PluginPersistenceSettings

case class EncryptionConfig(keyMap: Map[Int, Array[Byte]])

class Encryptor(key: Array[Byte]) {
  private val secretKey = new SecretKeySpec(key, "AES")
  private val cipher1 = Cipher.getInstance("AES")
  cipher1.init(Cipher.ENCRYPT_MODE, secretKey)
  private val cipher2 = Cipher.getInstance("AES")
  cipher2.init(Cipher.DECRYPT_MODE, secretKey)

  def encrypt(input: Array[Byte]): Array[Byte] = cipher1.doFinal(input)
  def decrypt(input: Array[Byte]): Array[Byte] = cipher2.doFinal(input)
}

class EncryptingSerializationExtension(system: ActorSystem, settingString: String) {
  private val ser: Serialization = SerializationExtension(system)
  val config = (new Eval()(settingString)).asInstanceOf[EncryptionConfig]
  assert(config.keyMap.nonEmpty)
  private val encryptors: Map[Int, Encryptor] = config.keyMap.map(kv => (kv._1, new Encryptor(kv._2)))
  private val defaultVersion: Int = encryptors.keySet.max
  private val defaultEncryptor: Encryptor = encryptors(defaultVersion)
  // println("EncryptingSerializationExtension config:" + "*" * 100 + config)

  def serialize(o: AnyRef) = {
    val raw = ser.serialize(o).get
    val encrypted = defaultEncryptor.encrypt(raw)

    val buffer = ByteBuffer.allocate(4 + encrypted.length)
    buffer.putInt(defaultVersion)
    buffer.put(encrypted)
    buffer.array
  }

  def deserialize[T](bytes: Array[Byte], clazz: Class[T]): T = {
    def toInt(bytes: Seq[Byte]): Int = bytes.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))

    val version = toInt(bytes.slice(0, 4))
    val encryptor = encryptors(version)
    val raw = encryptor.decrypt(bytes.drop(4))
    ser.deserialize(raw, clazz).get
  }
}

object EncryptingSerializationExtension {
  def apply(system: ActorSystem, settingString: String): EncryptingSerializationExtension = {
    new EncryptingSerializationExtension(system, settingString)
  }
}

///////////////////////
trait HBaseSerialization {
  self: Actor =>

  val settings: PluginPersistenceSettings
  lazy val serialization = EncryptingSerializationExtension(context.system, context.system.settings.config.getString("akka.persistence.encryption-settings"))

  protected def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr])

  protected def persistentToBytes(msg: Persistent): Array[Byte] =
    serialization.serialize(msg)
}
