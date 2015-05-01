package im.yanchen.pupgrowth

import scala.collection.mutable.ArrayBuffer

class UtilList {
  var item: Int = 0

  var iutilSum: Int = 0
  var rutilSum: Int = 0

  var tlist: ArrayBuffer[(Int, Int, Int)] = ArrayBuffer()


  def this(itemIn1: Int) {
    this()
    item = itemIn1
  }

  def addElement(tid: Int, iu: Int, ru: Int) {
    tlist.append((tid, iu, ru))
    iutilSum += iu
    rutilSum += ru
  }
}