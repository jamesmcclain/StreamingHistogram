package com.daystrom_data_concepts

import java.util.TreeMap
import java.util.Comparator
import scala.collection.JavaConverters._
import StreamingHistogram.{BucketType, DeltaType}


object StreamingHistogram {
  private type BucketType = (Double, Int)
  private type DeltaType = (Double, BucketType, BucketType)

  def apply(m: Int) = new StreamingHistogram(m, None, None)

  def apply(m: Int, buckets: TreeMap[Double, Int], deltas: TreeMap[DeltaType, Unit]) =
    new StreamingHistogram(m, Some(buckets), Some(deltas))
}

/**
 * Ben-Haim, Yael, and Elad Tom-Tov. "A streaming parallel decision
 * tree algorithm."  The Journal of Machine Learning Research 11
 * (2010): 849-872.
 */
class StreamingHistogram(
  m: Int,
  startingBuckets: Option[TreeMap[Double, Int]],
  startingDeltas: Option[TreeMap[DeltaType, Unit]]
) {

  class DeltaCompare extends Comparator[DeltaType] {
    def compare(a: DeltaType, b: DeltaType): Int =
      if (a._1 < b._1) -1
      else if (a._1 > b._1) 1
      else {
        if (a._2._1 < b._2._1) -1
        else if (a._2._1 > b._2._1) 1
        else 0
      }
  }

  private val buckets = startingBuckets.getOrElse(new TreeMap[Double, Int])
  private val deltas = startingDeltas.getOrElse(new TreeMap[DeltaType, Unit](new DeltaCompare))

  /* The number of samples represented by this histogram */
  var n = getBuckets.map(_._2).sum

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
   *
   * This function appropriately modifies both the buckets and the
   * deltas.  The deltas on either side of the collapsed pair are
   * removed and replaced with deltas meed the mid-point and
   * respective extremes.
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
   * Add a bucket to this histogram.  This can be used to add a new
   * sample to the histogram by letting the bucket-count be equal to
   * one, or it can be used to incrementally merge two histograms.
   */
  def update(b: BucketType): Unit = {
    /* First entry */
    if (buckets.size == 0)
      buckets.put(b._1, b._2)
    /* Duplicate entry */
    else if (buckets.containsKey(b._1))
      buckets.put(b._1, buckets.get(b._1) + b._2)
    /* Create new entry */
    else {
      val smaller = {
        val entry = buckets.lowerEntry(b._1)
        if (entry != null) Some(entry.getKey, entry.getValue); else None
      }
      val larger = {
        val entry = buckets.higherEntry(b._1)
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
        val delta = large._1 - b._1
        deltas.put((delta, b, large), Unit)
      }

      /* Add delta between new bucket and next-smallest bucket */
      if (smaller != None) {
        val small = smaller.get
        val delta = b._1 - small._1
        deltas.put((delta, small, b), Unit)
      }
    }

    n += b._2
    buckets.put(b._1, b._2)
    if (buckets.size > m) merge
  }

  /**
   * Additional update overloads.
   */
  def update(d: Double): Unit = update((d, 1))
  def update(bs: Seq[BucketType]): Unit = bs.foreach({ b => update(b) })
  def update(ds: Seq[Double])(implicit dummy: DummyImplicit): Unit = ds.foreach({ d => update((d, 1)) })

  /**
   * Combine operator.
   */
  def +(other: StreamingHistogram): StreamingHistogram = {
    val sh = StreamingHistogram(this.m, this.buckets, this.deltas)
    sh.update(other.getBuckets)
    sh
  }

  /**
   * For each q in qs, between 0 and 1, find a number (approximately)
   * at the qth percentile.
   */
  def getQuantiles(qs: Seq[Double]): Seq[Double] = {
    val bucketList = getBuckets
    val data = bucketList.map(_._1)
    val pdf = bucketList.map(_._2.toDouble / n).scanLeft(0.0)(_ + _).drop(1)
    qs.map({ q => data.zip(pdf).dropWhile(_._2 < q).head._1 })
  }

  def getQuantiles(k: Int): Seq[Double] = getQuantiles(List.range(0,k).map(_ / k.toDouble))

  def getQuantile(q: Double): Double = getQuantiles(List(q)).head

  def getBuckets(): List[BucketType] = buckets.asScala.toList

  def getDeltas(): List[DeltaType] = deltas.keySet.asScala.toList
}
