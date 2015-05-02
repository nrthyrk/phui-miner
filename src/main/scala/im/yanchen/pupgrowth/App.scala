package im.yanchen.pupgrowth

import org.apache.spark.util.{ BoundedPriorityQueue, Utils }
import scala.io.Source
import java.io._
import scala.collection.mutable._
import util.control.Breaks._
import scala.util._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd._
import scala.util.Sorting

/**
 * @author Yan Chen
 */
object App {
  def grouping(lists: Array[(String, Int)], tnum: Long, pnum: Int): Map[String, Int] = {
    val avg = (tnum * 1.0 / pnum).toInt
    var mp = Map[String, Int]()
    var bins = Array.fill[Int](pnum)(0)
    Sorting.quickSort(lists)(Ordering[Int].on(x => -x._2))
    for ((key, value) <- lists) {
      breakable {
        for (i <- 0 to pnum - 1) {
          if (bins(i) == 0 || i == pnum - 1 || (bins(i) < avg && bins(i) + value <= 1.5 * avg)) {
            println("############# assigning " + value + " to " + i + ", bin(i) = " + bins(i) + ", and avg = " + avg)
            mp(key) = i
            bins(i) += value
            break
          }
        }
      }
    }
    mp
  }
  def main(args: Array[String]) {
    val theta = args(1).toDouble
    val parNum = args(2).toInt
    val method = args(3).toInt // 0: print stats; 1: PUPGrowth
    val depth = args(4).toInt // 2
    val outputf = args(5)

    val conf = new SparkConf().setAppName("PUPGrowth")
      .set("spark.executor.memory", "11G")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "im.yanchen.pupgrowth.ARegistrator")
      .set("spark.kryoserializer.buffer.mb", "24")
      .set("spark.storage.blockManagerHeartBeatMs", "30000000")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0)).repartition(parNum)
    val transacs = lines.map(s => new Transaction(s))
    transacs.persist()

    // use count here for executing revisedTransacs
    // compute statistics
    val (utotal, tnum) = transacs.mapPartitions(x => {
      var maxutil = 0L
      var totalutil = 0L
      var count = 0L
      while (x.hasNext) {
        val ele = x.next()
        totalutil += ele.utility
        count += 1
      }
      Array((totalutil, count)).iterator
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    // compute item TWU, and filter unpromising ones
    val thresUtil = theta * utotal
    val thresUtilBroad = sc.broadcast(thresUtil)
    val itemTwu = transacs.flatMap(x => x.itemset.map(y => (y._1, x.utility)))
      .reduceByKey(_ + _)
      .filter(x => x._2 >= thresUtilBroad.value)
      .collect()
      .toMap
    val itemTwuBroad = sc.broadcast(itemTwu)
    val revisedTransacs = transacs.map(t => {
      t.itemset = t.itemset.filter(x => itemTwuBroad.value.get(x._1) != None)
      Sorting.quickSort(t.itemset)(Ordering[(Int, Int)].on(x => (itemTwuBroad.value.get(x._1).get, x._1)))
      t
    }).filter(x => x.itemset.size >= depth)
    revisedTransacs.persist()
    transacs.unpersist()
    
    val tnumn = revisedTransacs.count()

    val freqs = revisedTransacs.map(x => (x.itemset.slice(0, depth).map(x => x._1).mkString(" "), 1))
      .reduceByKey(_ + _)
      .collect()
      .toMap

    val glists = grouping(freqs.toArray, tnumn, parNum)

    if (method == 0) {
      var writer = new BufferedWriter(new FileWriter(outputf))
      writer.write("The number of transactions is " + tnum + "\n")
      writer.write("glists are\n")
      for ((key, value) <- glists) {
        writer.write("" + key + ": " + freqs(key) + ":" + value + "\n")
      }
      writer.write("# of transactions in each node is\n")
      var nums = Map[Int, Int]()
      for ((key, value) <- glists) {
        if (nums contains value) {
          nums(value) += freqs(key)
        } else {
          nums(value) = freqs(key)
        }
      }
      for ((key, value) <- nums) {
        writer.write("" + key + ": " + value + "\n")
      }
      writer.close()

    } else if (method == 1) {

    } else {
      println("Warning: method not implemented!")
    }

  }

}
