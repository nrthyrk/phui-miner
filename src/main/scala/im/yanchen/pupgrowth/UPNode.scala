package im.yanchen.pupgrowth

import scala.collection.mutable._

class UPNode {
  var itemID = -1
  var count = 1
  var nodeUtility: Int = -1
  var parent: UPNode = null
  var childs = ArrayBuffer[UPNode]()
  var nodeLink: UPNode = null
  
  def getChildWithID(name: Int): UPNode = {
    for (child <- childs) {
      if (child.itemID == name) {
        return child
      }
    }
    return null
  }
  
  override def toString(): String = {
    return "(i=" + itemID + " count=" + count + " nu=" + nodeUtility + ")"
  }
  
  def print() {
    for (child <- childs) {
      println("" + itemID + "->" + child.itemID)
    }
    for (child <- childs) {
      child.print()
    }
  }
  
}