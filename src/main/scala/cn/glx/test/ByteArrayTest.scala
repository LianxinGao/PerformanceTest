package cn.glx.test

import cn.glx.test.ByteArrayTest.{FIRST_BYTE, LAST_BYTES, MIDDLE_BYTES}
import org.apache.commons.io.FileUtils
import org.rocksdb.{Options, RocksDB}

import java.io.File
import scala.collection.mutable.ArrayBuffer

/**
 * @program: StorageTest
 * @description:
 * @author: LiamGao
 * @create: 2022-02-22 09:14
 */
object ByteArrayTest {
  val FIRST_BYTE = 1
  val MIDDLE_BYTES = 2
  val LAST_BYTES = 3

  def main(args: Array[String]): Unit = {
    val test = new ByteArrayTest
    test.initDB("./testdata/testdb.db")
    val filePath = args(0)
    val fileName = args(1)
    val times = args(2)
    val kind = args(3)
    test.testByteArrayWrite(filePath, fileName, 1)
    test.testByteArrayRead(fileName, times.toInt, kind.toInt)
    test.close()
  }
}

class ByteArrayTest {
  var db: RocksDB = _

  def initDB(path: String): Unit = {
    val option = new Options
    option.setCreateIfMissing(true)
    db = RocksDB.open(option, path)
    db.deleteRange(Array(0.byteValue()), Array((-1).byteValue()))
    db.put("warmup1".getBytes("utf-8"), "warm_up1".getBytes("utf-8"))
    db.put("warmup2".getBytes("utf-8"), "warm_up2".getBytes("utf-8"))
    db.put("warmup3".getBytes("utf-8"), "warm_up3".getBytes("utf-8"))
  }

  def testByteArrayWrite(filePath: String, key: String, times: Int): Unit = {
    val file = new File(filePath)
    val data = FileUtils.readFileToByteArray(file)
    val k = key.getBytes("utf-8")
    val start = System.currentTimeMillis()
    for (i <- 1 to times) {
      db.put(k, data)
    }
    val cost = System.currentTimeMillis() - start

    val res = Math.ceil(cost / times.toFloat)

    println(s"test write ${file.getName} file $times times, avg cost: $res ms")
  }

  def testByteArrayRead(key: String, times: Int, testType: Int): Unit = {
    val k = key.getBytes("utf-8")
    var timeArray = ArrayBuffer[Long]()
    testType match {
      case FIRST_BYTE => {
        for (i <- 1 to times) {
          val start = System.currentTimeMillis()
          val v = db.get(k)
          v.slice(0, 1)
          timeArray.append(System.currentTimeMillis() - start)
        }
        timeArray = timeArray.sorted
        val avg = Math.ceil(timeArray.sum / times.toFloat)
        println(s"test get $key file of bytes array's FIRST_BYTE $times times, max time: ${timeArray.last} ms, min time: ${timeArray.head} ms, avg cost: $avg ms")
      }
      case MIDDLE_BYTES => {
        for (i <- 1 to times) {
          val start = System.currentTimeMillis()
          val v = db.get(k)
          v.slice(0, v.length / 2)
          timeArray.append(System.currentTimeMillis() - start)
        }
        timeArray = timeArray.sorted
        val avg = Math.ceil(timeArray.sum / times.toFloat)
        println(s"test get $key file of bytes array's MIDDLE_BYTES $times times, max time: ${timeArray.last} ms, min time: ${timeArray.head} ms, avg cost: $avg ms")
      }
      case LAST_BYTES => {
        for (i <- 1 to times) {
          val start = System.currentTimeMillis()
          val v = db.get(k)
          v.slice(0, v.length)
          timeArray.append(System.currentTimeMillis() - start)
        }
        timeArray = timeArray.sorted
        val avg = Math.ceil(timeArray.sum / times.toFloat)
        println(s"test get $key file of bytes array's LAST_BYTES $times times, max time: ${timeArray.last} ms, min time: ${timeArray.head} ms, avg cost: $avg ms")
      }
    }
  }

  def close(): Unit = {
    db.close()
  }
}
