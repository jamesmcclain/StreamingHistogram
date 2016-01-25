package com.daystrom_data_concepts

import java.util.Arrays
import java.util.Comparator
import scala.collection.mutable.TreeSet


class StreamingHistogram(m: Int) {
  private type BucketType = (Double, Int)
  private type DeltaType = (Double, BucketType, BucketType)

  private val buckets = TreeSet.empty[BucketType]
  private val deltas = TreeSet.empty[DeltaType]

  def getBuckets(): List[BucketType] = buckets.toList

  def getDeltas(): List[DeltaType] = deltas.toList

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
    val delta = deltas.head
    val (_, middle1, middle2) = delta
    val middle = merge(middle1, middle2)
    val left = {
      val until = buckets.until(middle1)
      if (until.isEmpty) None; else until.lastOption
    }
    val right = {
      val from = buckets.from(middle2)
      if (from.isEmpty) None; else from.tail.headOption
    }

    /* remove delta between middle1 and middle2 */
    deltas.remove(delta)

    /* Replace delta to the left the merged buckets */
    if (left != None) {
      val other = left.get
      val oldDelta = middle1._1 - other._1
      val newDelta = middle._1 - other._1
      deltas.remove(oldDelta, other, middle1)
      deltas.add(newDelta, other, middle)
    }

    /* Replace delta to the right the merged buckets */
    if (right != None) {
      val other = right.get
      val oldDelta = other._1 - middle2._1
      val newDelta = other._1 - middle._1
      deltas.remove(oldDelta, middle2, other)
      deltas.add(newDelta, middle, other)
    }

    /* Replace merged buckets with their average */
    buckets.remove(middle1)
    buckets.remove(middle2)
    buckets.add(middle)
  }

  /**
   * Add a datum.
   */
  def update(d: Double): Unit = {
    val newBucket = (d, 1)
    val smaller = buckets.to(newBucket).lastOption
    val larger = buckets.from(newBucket).headOption

    /* First entry */
    if (smaller == None && larger == None) {
      buckets.add(newBucket)
    }
    /* Duplicate entry */
    else if (larger != None && larger.get._1 == d) {
      val large = larger.get
      buckets.remove(large)
      buckets.add((d, large._2+1))
    }
    /* Create new entry */
    else {
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
        deltas.add(delta, newBucket, large)
      }

      /* Add delta between new bucket and next-smallest bucket */
      if (smaller != None) {
        val small = smaller.get
        val delta = d - small._1
        deltas.add(delta, small, newBucket)
      }

      buckets.add(newBucket)
      if (buckets.size > m) merge
    }
  }
}
