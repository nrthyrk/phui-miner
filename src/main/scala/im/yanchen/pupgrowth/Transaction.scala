package im.yanchen.pupgrowth

import scala.collection.mutable._

class Transaction {
  var itemset: Array[(Int, Int)] = null
  var utility: Int = 0

  def this(line: String) = {
    this()
    val splits = line.trim().split(":")
    var items = splits(0).split(" ") map { _.toInt }
    var utils = splits(2).split(" ") map { _.toInt }
    itemset = items zip utils
    utility = splits(1).toInt
  }

  def length: Int = itemset.size

}