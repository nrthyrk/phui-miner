package im.yanchen.pupgrowth

import scala.collection.mutable.ArrayBuffer

class UtilityList {
  var itemset: ArrayBuffer[Int] = ArrayBuffer()

  var iutilSum: Int = 0
  var rutilSum: Int = 0

  var tlist: ArrayBuffer[(Int, Int, Int)] = ArrayBuffer()

  def this(itemsetIn: ArrayBuffer[Int]) {
    this()
    itemset = itemsetIn
  }

  def this(itemIn1: Int) {
    this()
    itemset.append(itemIn1)
  }

  def addElement(tid: Int, iu: Int, ru: Int) {
    tlist.append((tid, iu, ru))
    iutilSum += iu
    rutilSum += ru
  }
}