package im.yanchen.pupgrowth

import org.apache.spark._

class BinPartitioner[V](partitions: Int)
  extends Partitioner {
  
  def numPartitions: Int = partitions
  
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    return k
  }
}