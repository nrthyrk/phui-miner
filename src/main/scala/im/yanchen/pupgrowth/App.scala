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

/**
 * @author Yan Chen
 */
object App {
  def grouping(items: List[Int], pnum: Int): Map[Int, Int] = {
    
    var mp = Map[Int, Int]()
    
    var i = 0
    var inc = 1
    var flag = false
    for (x <- items) {
      mp.put(x, i)
      i += inc
      if (i == 0 || i == pnum-1) {
        if (flag == false) {
          inc = 0
          flag = true
        } else {
          if (i == 0)
            inc = 1
          else
            inc = -1
          flag = false
        }
      }
    }
    
    mp
  }
  
  def grouping2(itemTwu: scala.collection.immutable.Map[Int, Int], pnum: Int): Map[Int, Int] = {
    
    
    
    var sortedItemTwu = itemTwu.toList.sortBy(_._2)
    
//    val avg = (Math.pow(2, sortedItemTwu.length) - 1) / pnum
    var mp = Map[Int, Int]()
//    var counter = Array.fill(pnum)(0.0)
//    for (x <- itemTwu.keySet) {
//      if (counter(i) > avg && i + 1 < pnum) {
//        i += 1
//      }
//      mp(x) = i
//      counter(i) += 1
//    }
    
    for (i <- 0 to sortedItemTwu.length-1) {
      if (i < pnum-1) {
        mp(sortedItemTwu(i)._1) = i
      } else {
        mp(sortedItemTwu(i)._1) = pnum-1
      }
    }
    
    
    mp
  }
  
  def main(args: Array[String]) {
    
    var startTimestamp = System.currentTimeMillis()
    
    val theta = args(1).toDouble
    val parNum = args(2).toInt
    val method = args(3).toInt // 0: print stats; 1: PUPGrowth; 2: PUPGrowth with sampling
    val sperc = args(4).toDouble // sample size
    val outputf = args(5)

    val conf = new SparkConf().setAppName("PUPGrowth")
      .set("spark.executor.memory", "28G")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "im.yanchen.pupgrowth.ARegistrator")
      .set("spark.kryoserializer.buffer.mb", "24")
      .set("spark.storage.blockManagerHeartBeatMs", "30000000")
    val sc = new SparkContext(conf)

    var lines = sc.textFile(args(0)).repartition(parNum)
    if (method == 2) {
      lines = lines.sample(false, sperc, 0)
    }
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
    }).filter(x => x.itemset.size >= 1)
    revisedTransacs.persist()
    transacs.unpersist(false)
    
    val items = itemTwu.toList.sortBy(x => x._2).map(x => x._1)

    val glists = grouping(items, parNum)
    val glistsBroad = sc.broadcast(glists)

    if (method == 0) {
      var writer = new BufferedWriter(new FileWriter(outputf))
      writer.write("The number of transactions is " + tnum + "\n")
      writer.write("items list: " + items.mkString(" ") + "\n")
      writer.write("glists are\n")
      for ((key, value) <- glists) {
        writer.write("" + key + ":" + value + "\n")
      }
      writer.close()

    } else if (method == 1 || method == 2) {
      var kset = revisedTransacs.flatMap { x => {
        var tlist = ArrayBuffer[(Int, Array[(Int, Int)])]()
        var added = Map[Int, Boolean]()
        for (i <- 0 to x.length-1) {
          var firstitem = x.itemset(i)._1
          var gid = glistsBroad.value(firstitem)
          if (added.get(gid) == None) {
            tlist.append((gid, x.itemset.slice(i, x.length)))
            added(gid) = true
          }
        }
        tlist.iterator
      } }
      kset.persist()
      revisedTransacs.unpersist(false)
      
      kset = kset.partitionBy(new BinPartitioner(parNum))
      
      val gset = kset.groupByKey()
      
      val results = gset.flatMap(x => {
        var hm = new HUIMiner(itemTwuBroad.value)
        for (transac <- x._2) {
          hm.addTransac(transac)
        }
        
        hm.mine(thresUtilBroad.value.toInt, glistsBroad.value, x._1)
      })
      
      val fresults = results.collect().toMap
      
      var endTimestamp = System.currentTimeMillis()

      println("glists: ")
      for ((key, value) <- glists) {
        println("\t" + key + ": " + value)
      }
      println("Thres: " + thresUtil.toInt)
      println("Total HUIs: " + fresults.size)
      println("Running time: " + (endTimestamp - startTimestamp))
      
      var writer = new BufferedWriter(new FileWriter(outputf))
      
      for ((key, value) <- fresults) {
        writer.write("" + key + ": " + value + "\n")
      }
      writer.close()
    } else {
      println("Warning: method not implemented!")
    }

  }
  
  def updateExactUtility(transac: Array[(Int, Int)], itemset: Array[Int], mapItemToTWU: scala.collection.immutable.Map[Int, Int], results: Map[Array[Int], Int]) {
    var utility = 0
    
    for (i <- 0 to itemset.length-1) {
      breakable {
        var itemI = itemset(i)
        for (j <- 0 to transac.length-1) {
          var itemJ = transac(j)
          
          if (itemJ._1 == itemI) {
            utility += itemJ._2
            break
          } else if (mapItemToTWU(itemJ._1) < mapItemToTWU(itemI)) {
            return
          }
          
        }
        return
      }
    }
    
    if (!results.contains(itemset)) {
      results(itemset) = 0
    }
    results(itemset) += utility
    
  }

}
