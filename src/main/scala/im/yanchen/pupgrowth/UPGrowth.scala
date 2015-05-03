package im.yanchen.pupgrowth

import scala.collection.mutable._
import scala.util._
import util.control.Breaks._

class UPGrowth {
  
  var huiCount = 0
  var phuisCount: Int = -1
  
  var mapMinimumItemUtility = Map[Int, Int]()
  var phuis = ArrayBuffer[Array[Int]]()
  
  var tree = new UPTree()
  
  def addTransac(t: Transaction) {
    
    var remainingUtility = 0
    
    for ((item, util) <- t.itemset) {
      remainingUtility += util
      var minItemUtil = mapMinimumItemUtility.get(item)
      if (minItemUtil == None || minItemUtil.get >= util) {
        mapMinimumItemUtility(item) = util
      }
    }
    
    //println(t.itemset.mkString(" "))
    
    tree.addTransaction(t, remainingUtility)
  }
  
  def run(mapItemToTWU: scala.collection.immutable.Map[Int, Int], minUtility: Int) {
    
    tree.root.print()
    
    tree.createHeaderList(mapItemToTWU)
    
    println(tree.headerList.mkString(" "))
    
    upgrowth(tree, minUtility, new Array[Int](0), mapItemToTWU)
    
    phuisCount = phuis.size
  }
  
  def upgrowth(tree: UPTree, minUtility: Int, prefix: Array[Int], mapItemToTWU: scala.collection.immutable.Map[Int, Int]) {
    for (i <- tree.headerList.size-1 to 0 by -1) {
      
      //println("#2 " )
      
      var item = tree.headerList(i)
      
      var localTree = createLocalTree(minUtility, tree, item)
      
      var pathCPBOpt = tree.mapItemNodes.get(item)
      
      var pathCPBUtility = 0
      while (pathCPBOpt != None && pathCPBOpt.get != null) {
        pathCPBUtility += pathCPBOpt.get.nodeUtility
        pathCPBOpt = Option(pathCPBOpt.get.nodeLink)
      }
      
      if (pathCPBUtility >= minUtility) {
        var newPrefix = new Array[Int](prefix.length + 1)
        Array.copy(prefix, 0, newPrefix, 0, prefix.length)
        newPrefix(prefix.length) = item
        
        //println("#1: " + newPrefix.toString() )
        
        savePHUI(newPrefix, mapItemToTWU)
        
        if (localTree.headerList.size > 0) {
          upgrowth(localTree, minUtility, newPrefix, mapItemToTWU)
        }
      }
      
    }
  }
  
  def savePHUI(itemset: Array[Int], mapItemToTWU: scala.collection.immutable.Map[Int, Int]) {
    Sorting.quickSort[Int](itemset)(Ordering[(Int, Int)].on(x => (-mapItemToTWU(x), x)))
    phuis.append(itemset)
    //println("#3: " + itemset.mkString(" ") + ", ")
  }
  
  def createLocalTree(minUtility: Int, tree: UPTree, item: Int): UPTree = {
    var prefixPaths = ArrayBuffer[ArrayBuffer[UPNode]]()
    var pathOpt = tree.mapItemNodes.get(item)
    
    var itemPathUtility = Map[Int, Int]()
    breakable {
      while (pathOpt != None && pathOpt.get != null) {
        var path = pathOpt.get
        
        var nodeutility = path.nodeUtility
        
        if (path.parent.itemID != -1) {
          var prefixPath = ArrayBuffer[UPNode]()
          prefixPath.append(path)
          var parentnode = path.parent
          while (parentnode.itemID != -1) {
            prefixPath.append(parentnode)
            var puOpt = itemPathUtility.get(parentnode.itemID)
            var pu = if (puOpt == None) nodeutility else puOpt.get + nodeutility
            itemPathUtility(parentnode.itemID) = pu
            parentnode = parentnode.parent
          }
          prefixPaths.append(prefixPath)
        }
        
        pathOpt = Option(path.nodeLink)
      }
    }
    
    var localTree = new UPTree()
    
    for (prefixPath <- prefixPaths) {
      var pathCount = prefixPath(0).count
      var pathUtility = prefixPath(0).nodeUtility
      
      var localPath = ArrayBuffer[Int]()
      
      for (j <- 1 to prefixPath.size-1) {
        var itemValue = 0
        var node = prefixPath(j)
        
        if (itemPathUtility(node.itemID) >= minUtility) {
          localPath.append(node.itemID)
        } else {
          var minItemUtility = mapMinimumItemUtility(node.itemID)
          itemValue = minItemUtility * pathCount
        }
        pathUtility = pathUtility - itemValue
        
      }
            
      localPath = localPath.sortBy { x => (-itemPathUtility(x), x) }
      
      localTree.addLocalTransaction(localPath, pathUtility, mapMinimumItemUtility, pathCount)
    }
    
    localTree.createHeaderList(itemPathUtility.toMap)
    
    localTree
  }
}