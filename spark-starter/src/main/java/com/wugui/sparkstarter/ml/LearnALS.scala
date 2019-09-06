package com.wugui.sparkstarter.ml

import org.apache.spark.sql.{Encoders, SparkSession}



object LearnALS {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ml").master("local").getOrCreate()

    val ratings = spark.read.textFile("/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/sample_movielens_ratings.txt")
      .map(parseRating)(Encoders.product)
      .toDF()
    ratings.show()

    // df 按比例分成训练集和测试集
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))



  }

  /**
    * 解析每行数据返回封装好的Rating对象
    */
  def parseRating(str: String): Rating = {
    val fields = str.split(":")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }
}
