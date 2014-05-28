package akka.persistence.hbase.common

import Const._

object ShortStringGenerator {

  def main(args: Array[String]) {
    batchTest
  }

  def batchTest() {
    val ss = Array[String]("p_a", "p_aa", "p_v", "v_a", "v_aa", "v_am")
    for (s <- ss) {
      println(s"${s} => ${genMd5String(s)}")
    }
  }

  def normalTest() {
    val sLongUrl: String = "http://tech.sina.com.cn/i/2011-03-23/11285321288.shtml"
    val aResult: Array[String] = shortString(sLongUrl)
    // 打印出结果
    for (i <- 0 until aResult.length) {
      println(s"[${i}]:::${aResult(i)}")
    }
  }

  def shortString(raw: String): Array[String] = {
    // 可以自定义生成 MD5 加密字符传前的混合 KEY
    val key: String = "coinport"
    // 要使用生成 URL 的字符
    val chars: Array[String] = Array("a", "b", "c", "d", "e", "f", "g", "h",
      "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
      "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5",
      "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H",
      "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T",
      "U", "V", "W", "X", "Y", "Z")
    // 对传入网址进行 MD5 加密
    val sMD5EncryptResult = (new Md5()).getMD5ofStr(key + raw)
    //    println("sMD5EncryptResult => " + sMD5EncryptResult)
    val hex: String = sMD5EncryptResult
    val resStrings = new Array[String](4)
    for (i <- 0 until 4) {
      // 把加密字符按照 8 位一组 16 进制与 0x3FFFFFFF 进行位与运算
      val sTempSubString = hex.substring(i * 8, i * 8 + 8)
      // 这里需要使用 long 型来转换，因为 Inteper.parseInt() 只能处理 31 位 , 首位为符号位 , 如果不用 long ，则会越界
      var lHexLong = 0x3FFFFFFF & java.lang.Long.parseLong(sTempSubString, 16)
      val outChars = new StringBuilder
      for (j <- 0 until 6) {
        // 把得到的值与 0x0000003D 进行位与运算，取得字符数组 chars 索引
        val index: Int = (0x0000003D & lHexLong).asInstanceOf[Int]
        // 把取得的字符相加
        outChars.append(chars(index))
        // 每次循环按位右移 5 位
        lHexLong = lHexLong >> 5
      }
      // 把字符串存入对应索引的输出数组
      resStrings(i) = outChars.toString()
    }
    resStrings
  }

  def genMd5String(raw: String): String = {
    val resStrings = shortString(raw).toList
    resStrings(0) + resStrings(1)
  }
}

