package com.wugui.sparkstarter.ml

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

object StatisticsExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StatisticsApp")
    val sc = new SparkContext(conf)
    val classpath = this.getClass.getResource("/").getPath

    val txt = sc.textFile(classpath + "北京降雨量.txt")
    // 将数据转成向量vector的RDD
    val data = txt.flatMap(line => line.split(",")).map { s =>
      Vectors.dense(s.toDouble)
    }

    val summary = Statistics.colStats(data)
    // 每一列的平均值
    println(summary.mean)
    // 方差
    println(summary.variance)
    // 非0数量
    println(summary.numNonzeros)
    // 最大值
    println(summary.max)
  }

}
