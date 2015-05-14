package im.yanchen.pupgrowth

import scala.util._
import scala.collection.mutable._
import util.control.Breaks._

class HUIMiner private () {

  var itemTwu: scala.collection.immutable.Map[Int, Int] = null

  var mapItemToUls: HashMap[Int, UtilList] = HashMap()

  var tid = 0

  var results = ArrayBuffer[(String, Double)]()
  
  var candidateCount: Int = 0
  
  var uls: ArrayBuffer[UtilList] = ArrayBuffer()
  
  var mapFMAP: Map[Int, Map[Int, Int]] = new HashMap()

  def this(itemTwu: scala.collection.immutable.Map[Int, Int]) {
    this()
    this.itemTwu = itemTwu

    itemTwu.toList.sortBy(_._2) foreach {
      case (key, value) =>
        {
          var ul: UtilList = new UtilList(key)
          uls.append(ul)
          mapItemToUls.put(key, ul)
        }
    }
  }

  def addTransac(t: Array[(Int, Int)]) {
    // construct first UtilList
    var remainingUtility: Int = 0
    var newTWU = 0
    for(i <- 0 until t.length) {
      var pair = t(i)
      // add it
      remainingUtility += pair._2
      newTWU += pair._2
    }
    
    for (i <- 0 until t.size) {
      val (key, value) = t(i)
      remainingUtility -= value
      var ul: UtilList = mapItemToUls(key)
      ul.addElement(tid, value, remainingUtility)
      
      // BEGIN OPTIMIZATION FOR FHM
      var mapFMAPItemOpt = mapFMAP.get(key)
      
      var mapFMAPItem: Map[Int, Int] = null
      
      if (mapFMAPItemOpt.isEmpty) {
        mapFMAPItem = new HashMap[Int, Int]()
        mapFMAP.put(key, mapFMAPItem)
      } else {
        mapFMAPItem = mapFMAPItemOpt.get
      }
      
      for (j <- i+1 until t.size) {
        var pairAfter = t(j)
        var twuSumOpt = mapFMAPItem.get(pairAfter._1)
        if (twuSumOpt.isEmpty) {
          mapFMAPItem.put(pairAfter._1, newTWU)
        } else {
          mapFMAPItem.put(pairAfter._1, twuSumOpt.get + newTWU)
        }
      }
      // END OPT
    }
    tid += 1
  }

  def mine(thresUtil: Int, glists: Map[List[Int], Int], gid: Int, depth: Int): Iterator[(String, Double)] = {

    huiMiner(new Array[Int](0), null, uls, thresUtil, glists, gid, depth)
    
    results.iterator
  }
  
  def huiMiner(prefix: Array[Int], pUL: UtilList, 
      ULs: ArrayBuffer[UtilList], minUtility: Int, 
      glists: Map[List[Int], Int], gid: Int, depth: Int) {
    // For each extension X of prefix P
    
    for(i <- 0 until ULs.size){
      
      var X = ULs(i)
      
      if (prefix.length != depth-1 || glists(prefix.toList :+ X.item) == gid) {
        // If pX is a high utility itemset.
        // we save the itemset:  pX 
        if(X.iutilSum >= minUtility){
          // save to arraybuffer
          //writeOut(prefix, X.item, X.iutilSum);
          results.append((prefix.mkString(" ") + (if (prefix.isEmpty) "" else " ") + X.item, X.iutilSum))
        }
        
        // If the sum of the remaining utilities for pX
        // is higher than minUtility, we explore extensions of pX.
        // (this is the pruning condition)
        if(X.iutilSum + X.rutilSum >= minUtility){
          // This list will contain the utility lists of pX extensions.
          var exULs = new ArrayBuffer[UtilList]()
          // For each extension of p appearing
          // after X according to the ascending order
          for(j <- i+1 until ULs.size){
            breakable {
              var Y = ULs(j)
              
              // NEW
              
              var mapTWUFOpt = mapFMAP.get(X.item)
              
              if (!mapTWUFOpt.isEmpty) {
                var twuFOpt = mapTWUFOpt.get.get(Y.item)
                if (!twuFOpt.isEmpty && twuFOpt.get < minUtility) {
                  break
                }
              }
              
              candidateCount += 1
              
              // END NEW
              
              // we construct the extension pXY 
              // and add it to the list of extensions of pX
              exULs.append(AUtil.construct(pUL, X, Y))
            }
          }
          // We create new prefix pX
          var newPrefix = new Array[Int](prefix.length+1)
          System.arraycopy(prefix, 0, newPrefix, 0, prefix.length)
          newPrefix(prefix.length) = X.item
          
          // We make a recursive call to discover all itemsets with the prefix pXY
          huiMiner(newPrefix, X, exULs, minUtility, glists, gid, depth)
        }
      }
    }
  }



}