package im.yanchen.pupgrowth

import scala.collection.mutable._

class UPTree {
  
  var headerList: ArrayBuffer[Int] = null
  
  var hasMoreThanOnePath: Boolean = false
  
  var mapItemNodes = Map[Int, UPNode]()
  
  var root: UPNode = new UPNode()
  
  var mapItemLastNode = Map[Int, UPNode]()
  
  def addTransaction(t: Transaction, rtu: Int) {
    var currNode = root
    //var i = 0
    var ru = 0
    var size = t.itemset.size
    
    for (i <- 0 to size-1) {
      for (k <- i+1 to t.itemset.size-1) {
        ru += t.itemset(k)._2
      }
      var item = t.itemset(i)._1
      var child = currNode.getChildWithID(item)
      
      if (child == null) {
        var nu = rtu - ru
        ru = 0
        currNode = insertNewNode(currNode, item, nu)
      } else {
        var currnu = child.nodeUtility
        var nu = currnu + (rtu - ru)
        ru = 0
        child.count += 1
        child.nodeUtility = nu
        currNode = child
      }
      
    }
    
  }
  
  def addLocalTransaction(localPath: ArrayBuffer[Int], pathUtility: Int, 
      mapMinimumItemUtility: Map[Int, Int], pathCount: Int) {
    var currLocalNode = root
    //var i = 0
    var ru = 0
    var size = localPath.length
    
    for (i <- 0 to size-1) {
      for (k <- i+1 to localPath.length-1) {
        ru += mapMinimumItemUtility(localPath(k)) * pathCount
      }
      var item = localPath(i)
      var child = currLocalNode.getChildWithID(item)
      
      if (child == null) {
        var nu = pathUtility - ru
        ru = 0
        currLocalNode = insertNewNode(currLocalNode, item, nu)
      } else {
        var currnu = child.nodeUtility
        var nu = currnu + (pathUtility - ru)
        ru = 0
        child.count += 1
        child.nodeUtility = nu
        currLocalNode = child
      }
      
    }
    
  }
  
  def insertNewNode(currentLocalNode: UPNode, item: Int, nodeUtility: Int): UPNode = {
    var newNode = new UPNode();
    newNode.itemID = item
    newNode.nodeUtility = nodeUtility
    newNode.count = 1
    newNode.parent = currentLocalNode
    
    currentLocalNode.childs.append(newNode)
    
    if (!hasMoreThanOnePath && currentLocalNode.childs.size > 1) {
      hasMoreThanOnePath = true;
    }
    
    var localHeaderNode = mapItemNodes.get(item)
    
    if (localHeaderNode == None) {
      mapItemNodes(item) = newNode
      mapItemLastNode(item) = newNode
    } else {
      var lastNode = mapItemLastNode(item)
      lastNode.nodeLink = newNode
      mapItemLastNode(item) = newNode
    }
    
    newNode
  }
  
  def createHeaderList(mapItemToEstimatedUtility: scala.collection.immutable.Map[Int, Int]) {
    headerList = ArrayBuffer[Int]()
    headerList.appendAll(mapItemNodes.keys)
    headerList = headerList.sortBy { x => ( -mapItemToEstimatedUtility(x), x ) }
  }
  
}