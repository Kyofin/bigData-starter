package com.wugui.sparkstarter.ml

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.stat.Statistics

object MatrixExample {

  def main(args: Array[String]): Unit = {
    // 矩阵打竖排
    val dm  = Matrices.dense(3,2,Array(1,2,3,4,5,6))
    println(dm)

    // 矩阵打横排
    val d1 = DenseMatrix(Array(1,2),Array(3,4),Array(5,6))
    println(d1)

    // 矩阵转置
    println(d1.t)

    // 皮尔森卡方检验
    val pValue = Statistics.chiSqTest(Matrices.dense(2,2,Array(127,19,147,10)))
    println(pValue)
  }
}
