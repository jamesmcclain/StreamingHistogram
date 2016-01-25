package com.daystrom_data_concepts

import scala.util.Random

object Main {
  val limit = 10000000
  val size = 500
  val r = Random
  val sh = new StreamingHistogram(size)

  def main(args: Array[String]) = {
    Iterator.range(0,limit).foreach({ i => sh.update(r.nextGaussian) })
    println
    println(s"${sh.getBuckets.size} buckets=${sh.getBuckets}")
    println
    println(s"${sh.getDeltas.size} deltas=${sh.getDeltas}")
  }
}
