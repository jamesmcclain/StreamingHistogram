package com.daystrom_data_concepts

import java.util.TreeMap
import java.util.Comparator
import scala.collection.JavaConverters._


class StreamingHistogram(m: Int) {
  private type BucketType = (Double, Int)
  private type DeltaType = (Double, BucketType, BucketType)

  class DeltaCompare extends Comparator[DeltaType] {
    def compare(a: DeltaType, b: DeltaType): Int =
      if (a._1 < b._1) -1
      else if (a._1 > b._1) 1
      else 0
  }

  private val buckets = new TreeMap[Double, Int]
  private val deltas = new TreeMap[DeltaType, Unit](new DeltaCompare)

  def getBuckets(): List[BucketType] = buckets.asScala.toList

  def getDeltas(): List[DeltaType] = deltas.keySet.asScala.toList

  /**
   * Take two buckets and return their composite.
   */
  private def merge(left: BucketType, right: BucketType): BucketType = {
    val (value1, count1) = left
    val (value2, count2) = right
    ((value1*count1 + value2*count2)/(count1 + count2), (count1 + count2))
  }

  /**
   * Merge the two closest-together buckets.
   *
   * Before: left ----- middle1 ----- middle2 ----- right
   *
   * After: left ------------- middle ------------- right
   */
  private def merge(): Unit = {
    val delta = deltas.firstKey
    val (_, middle1, middle2) = delta
    val middle = merge(middle1, middle2)
    val left = {
      val entry = buckets.lowerEntry(middle1._1)
      if (entry != null) Some(entry.getKey, entry.getValue); else None
    }
    val right = {
      val entry = buckets.higherEntry(middle2._1)
      if (entry != null) Some(entry.getKey, entry.getValue); else None
    }

    /* remove delta between middle1 and middle2 */
    deltas.remove(delta)

    /* Replace delta to the left the merged buckets */
    if (left != None) {
      val other = left.get
      val oldDelta = middle1._1 - other._1
      val newDelta = middle._1 - other._1
      deltas.remove((oldDelta, other, middle1))
      deltas.put((newDelta, other, middle), Unit)
    }

    /* Replace delta to the right the merged buckets */
    if (right != None) {
      val other = right.get
      val oldDelta = other._1 - middle2._1
      val newDelta = other._1 - middle._1
      deltas.remove((oldDelta, middle2, other))
      deltas.put((newDelta, middle, other), Unit)
    }

    /* Replace merged buckets with their average */
    buckets.remove(middle1._1)
    buckets.remove(middle2._1)
    buckets.put(middle._1, middle._2)
  }

  /**
   * Add a datum.
   */
  def update(d: Double): Unit = {
    /* First entry */
    if (buckets.size == 0) buckets.put(d, 1)
    /* Duplicate entry */
    else if (buckets.containsKey(d)) {
      val count = buckets.get(d)
      buckets.put(d, count + 1)
    }
    /* Create new entry */
    else {
      val smaller = {
        val entry = buckets.lowerEntry(d)
        if (entry != null) Some(entry.getKey, entry.getValue); else None
      }
      val larger = {
        val entry = buckets.higherEntry(d)
        if (entry != null) Some(entry.getKey, entry.getValue); else None
      }

      /* Remove delta containing new bucket */
      if (smaller != None && larger != None) {
        val large = larger.get
        val small = smaller.get
        val delta = large._1 - small._1
        deltas.remove((delta, small, large))
      }

      /* Add delta between new bucket and next-largest bucket */
      if (larger != None) {
        val large = larger.get
        val delta = large._1 - d
        deltas.put((delta, (d, 1), large), Unit)
      }

      /* Add delta between new bucket and next-smallest bucket */
      if (smaller != None) {
        val small = smaller.get
        val delta = d - small._1
        deltas.put((delta, small, (d, 1)), Unit)
      }

      buckets.put(d, 1)
      if (buckets.size > m) merge
    }
  }
}
