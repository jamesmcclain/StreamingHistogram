package com.daystrom_data_concepts

import scala.util.Random

object Main {
  val limit = 1000000
  val size = 80
  val r = Random
  val rs1 = Array.ofDim[Double](limit)
  val rs2 = Array.ofDim[Double](limit)
  val sh1 = StreamingHistogram(size)
  val sh2 = StreamingHistogram(size)

  def main(args: Array[String]) = {
    var i = 0

    while (i < limit) {
      rs1(i) = r.nextGaussian
      rs2(i) = r.nextGaussian
      i += 1
    }

    sh1.update(rs1)
    sh2.update(rs2)
    val sh3 = sh1 + sh2
    println(s"quantiles = ${sh3.getQuantiles(10)}")
    println(s"75th percentile = ${sh3.getQuantile(0.75)}")
  }
}
