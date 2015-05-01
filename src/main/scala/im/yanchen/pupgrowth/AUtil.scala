package im.yanchen.pupgrowth

import scala.util._
import scala.collection.mutable._

trait AUtil {
  def construct(prev: UtilList, left: UtilList, right: UtilList): UtilList = {

    var ul: UtilList = new UtilList(right.item)

    var leftIdx: Int = 0
    var rightIdx: Int = 0
    var prevIdx: Int = 0

    while (leftIdx < left.tlist.size && rightIdx < right.tlist.size) {
      var leftE = left.tlist(leftIdx)
      var rightE = right.tlist(rightIdx)
      var diff: Int = leftE._1.compareTo(rightE._1)
      if (diff == 0) {
        var iu: Int = leftE._2 + rightE._2
        var ru: Int = rightE._3
        if (prev == null) {
          ul.addElement(leftE._1, iu, ru)
        } else {
          // scan through prev until tid matches
          while (prev.tlist(prevIdx)._1 != leftE._1) {
            prevIdx += 1
          }
          ul.addElement(left.tlist(leftIdx)._1, iu - prev.tlist(prevIdx)._2, ru)
        }

        leftIdx += 1
        rightIdx += 1
      } else if (diff < 0) {
        leftIdx += 1
      } else {
        rightIdx += 1
      }
    }
    return ul
  }
  
    def constructs(prev: Option[UtilityList], left: UtilityList, right: UtilityList): UtilityList = {

    var itemset: ArrayBuffer[Int] = left.itemset.clone()
    itemset.append(right.itemset.last)

    var ul: UtilityList = new UtilityList(itemset)

    var leftIdx: Int = 0
    var rightIdx: Int = 0
    var prevIdx: Int = 0

    while (leftIdx < left.tlist.size && rightIdx < right.tlist.size) {
      var leftE = left.tlist(leftIdx)
      var rightE = right.tlist(rightIdx)
      var diff: Int = leftE._1.compareTo(rightE._1)
      if (diff == 0) {
        var iu: Int = leftE._2 + rightE._2
        var ru: Int = rightE._3
        if (prev.isEmpty) {
          ul.addElement(leftE._1, iu, ru)
        } else {
          // scan through prev until tid matches
          while (prev.get.tlist(prevIdx)._1 != leftE._1) {
            prevIdx += 1
          }
          ul.addElement(left.tlist(leftIdx)._1, iu - prev.get.tlist(prevIdx)._2, ru)
        }

        leftIdx += 1
        rightIdx += 1
      } else if (diff < 0) {
        leftIdx += 1
      } else {
        rightIdx += 1
      }
    }
    return ul
  }
}

object AUtil extends AUtil