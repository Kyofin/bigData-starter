package com.wugui.sparkstarter.ml
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors

object VectorsExample {
  def main(args: Array[String]): Unit = {
    val dv  = Vectors.dense(1,2,3)

    val denseVector1 = DenseVector(1,2,3)
    val denseVector2 = DenseVector(1,2,3)

    val sub =denseVector1 + denseVector2
    println(sub)

    println()

    // 向量乘法
    println(denseVector1 * denseVector2)
    println(denseVector1 * denseVector2.t)
  }

}
