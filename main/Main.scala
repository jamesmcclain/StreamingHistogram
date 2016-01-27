package com.daystrom_data_concepts

import scala.util.Random

object Main {
  val limit = 10000000
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

    sh1.countItems(rs1)
    sh2.countItems(rs2)
    val sh3 = sh1 + sh2
    println(s"Some quantiles = ${sh3.getQuantileBreaks(10)}")
    println(s"mode=${sh3.getMode} median=${sh3.getMedian} mean=${sh3.getMean}")
    println(s"${sh3.getPercentile(0.75)} is bigger than 75 percent of items")
    println(s"0.0 is bigger than ${100*sh3.getPercentileRanking(0.0)} percent of items")
  }
}
