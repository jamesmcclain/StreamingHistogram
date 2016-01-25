package com.daystrom_data_concepts

import scala.util.Random

object Main {
  val limit = 1000000
  val size = 1000
  val r = Random
  val sh = new StreamingHistogram(size)

  def main(args: Array[String]) = {
    var control = 0.0

    Iterator.range(0,limit).foreach({ i => sh.update(r.nextGaussian) })
    println
    // println(s"buckets=${sh.getBuckets} \ndeltas=${sh.getDeltas}")
    // println(s"${sh.ping}")
  }
}
