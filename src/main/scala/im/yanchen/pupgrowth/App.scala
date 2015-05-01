package im.yanchen.pupgrowth

import org.apache.spark.util.{BoundedPriorityQueue, Utils}
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
  def main(args: Array[String]) {
    val theta = args(1).toDouble
    val parNum = args(2).toInt
    val method = args(3).toInt // 0: print stats; 1: PUPGrowth
    val outputf = args(4)

    val conf = new SparkConf().setAppName("PUPGrowth")
      .set("spark.executor.memory", "29000M")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "im.yanchen.pupgrowth.ARegistrator")
      .set("spark.kryoserializer.buffer.mb", "24")
      .set("spark.storage.blockManagerHeartBeatMs", "30000000")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0)).repartition(parNum)
    val transacs = lines.map(s => new Transaction(s))
    transacs.persist()
    
    if (method == 0) {
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
      })
      transacs.unpersist()
      
      val freqs = revisedTransacs.map(x => (x.itemset(0)._1, 1))
        .reduceByKey(_ + _)
        .collect()
        .toMap
      
      var writer = new BufferedWriter(new FileWriter(outputf))
      writer.write("The number of transactions is " + tnum + "\n")
      writer.write("The number of items is " + freqs.size + "\n")
      for ((key, value) <- freqs) {
        writer.write("" + key + ": " + value + "\n")
      }
      writer.close()
      
    } else if (method == 1) {

    } else {
      println("Warning: method not implemented!")
    }

  }

}
