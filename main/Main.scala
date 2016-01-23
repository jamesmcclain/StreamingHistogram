package com.daystrom_data_concepts

import scala.util.Random

object Main {
  val limit = 10000000
  val size = 100
  val r = Random
  val sh = new StreamingHistogram(size)

  def main(args: Array[String]) = {

    Iterator.range(0,limit).foreach({ i => sh.update(r.nextGaussian) })
    println(sh.getBuckets)
  }
}
