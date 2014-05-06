package akka.persistence.hbase.common

import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import akka.actor.{ Actor, ActorSystem }
import akka.persistence.{ Persistent, PersistentRepr }
import akka.serialization.{ SerializationExtension, Serialization }
import akka.persistence.hbase.journal.PluginPersistenceSettings
import com.twitter.util.Eval
import java.nio.ByteBuffer

case class EncryptionConfig(keyMap: Map[Int, Array[Byte]])

class Encryptor(key: Array[Byte]) {
  private val secretKey = new SecretKeySpec(key, "AES")

  def encrypt(input: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.ENCRYPT_MODE, secretKey)
    cipher.doFinal(input)
  }

  def decrypt(input: Array[Byte]): Array[Byte] = {
    val cipher = Cipher.getInstance("AES")
    cipher.init(Cipher.DECRYPT_MODE, secretKey)
    cipher.doFinal(input)
  }
}

object EncryptingSerializationExtension {
  private var ser: Serialization = null
  private var encryptors: Map[Int, Encryptor] = null
  private var defaultEncryptor: Encryptor = null
  private var defaultVersion: Int = -1

  def apply(system: ActorSystem, settingString: String) = {
    ser = SerializationExtension(system)
    val config = (new Eval()(settingString)).asInstanceOf[EncryptionConfig]

    assert(config.keyMap.nonEmpty)

    println("EncryptingSerializationExtension config:" + "*" * 100 + config)
    encryptors = config.keyMap.map(kv => (kv._1, new Encryptor(kv._2)))
    defaultVersion = encryptors.keySet.max
    defaultEncryptor = encryptors(defaultVersion)
    this
  }

  def serialize(o: AnyRef) = {
    val raw = ser.serialize(o).get
    println("raw string => " + raw.map(_.toChar))
    val encrypted = defaultEncryptor.encrypt(raw)

    val buffer = ByteBuffer.allocate(4 + encrypted.length)
    buffer.putInt(defaultVersion)
    buffer.put(encrypted)
    val x = buffer.array
    println("encrypted string => " + x.map(_.toChar))
    x
  }

  def deserialize[T](bytes: Array[Byte], clazz: Class[T]): T = {
    def toInt(bytes: Seq[Byte]): Int = bytes.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))
    val version = toInt(bytes.slice(0, 4))
    println("decrypte: version " + version)
    val encryptor = encryptors(version)
    val raw = encryptor.decrypt(bytes.drop(4))
    println("raw after decryption: " + raw.map(_.toChar))
    ser.deserialize(raw, clazz).get
  }
}

///////////////////////
trait HBaseSerialization {
  self: Actor =>

  val settings: PluginPersistenceSettings
  lazy val serialization = EncryptingSerializationExtension(context.system, settings.encryptionSettingString)

  protected def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr])

  protected def persistentToBytes(msg: Persistent): Array[Byte] =
    serialization.serialize(msg)
}
