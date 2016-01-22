package com.daystrom_data_concepts

import java.util.Arrays
import java.util.Comparator


class StreamingHistogram(m: Int) {
  private type EntryType = (Double, Int)

  private val buckets = Array.ofDim[EntryType](m+1)
  private val cmp = new IndexCompare
  private var occupancy = 0

  private class IndexCompare extends Comparator[EntryType] {
    def compare(left: EntryType, right: EntryType): Int = {
      val leftValue = left._1
      val rightValue = right._1

      if (leftValue < rightValue) -1
      else if (leftValue > rightValue) 1
      else 0
    }
  }

  /**
   * Return a copy of the buckets.al
   */
  def getBuckets(): List[EntryType] = buckets.take(m).toList

  /**
   * Compute the merged bucket of two given buckets.
   */
  private def merge(i: Int, j: Int): EntryType = {
    val (q1, k1) = buckets(i)
    val (q2, k2) = buckets(j)

    ((q1*k1 + q2*k2)/(k1 + k2) , (k1 + k2))
  }

  /**
   * Update
   */
  def update(d: Double): Unit = {
    val entry = (d,1)

    if (occupancy < m) {
      buckets(occupancy) = entry
      occupancy += 1
    }
    else if (occupancy >= m) {
      var i = 0
      var smallest = Double.PositiveInfinity
      var index = -1

      /* Update the permutation pi for this iteration */
      buckets(m) = entry
      Arrays.sort(buckets, cmp)

      /* Find where merger should take place */
      while (i < m-1) {
        val gap = buckets(i+1)._1 - buckets(i)._1
        if (gap < smallest) {
          smallest = gap
          index = i
        }
        i += 1
      }

      /* Merge at appropriate place, clear entry that it merged with, and
       * compactify array. */
      buckets(index+0) = merge(index, index+1)
      buckets(index+1) = (Double.PositiveInfinity, -1)
      Arrays.sort(buckets, cmp)
    }

  }

}
