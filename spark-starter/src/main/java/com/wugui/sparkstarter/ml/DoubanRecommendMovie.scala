package com.wugui.sparkstarter.ml

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



object DoubanRecommendMovie {

  /**
    * 定义电影评价样本类
    *
    * @param userID
    * @param movieID
    * @param rating
    */
  case class MovieRating(userID: String, movieID: Int, rating: Double) extends scala.Serializable


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("DoubanRecommender").setMaster("local"))
    //数据读取目录
    val base = if (args.length > 0) args(0) else "/Users/huzekang/study/bigdata-starter/spark-starter/src/main/resources/data/"

    //获取RDD
    // <userID>:<movieID>:<评分>
    val rawUserMoviesData = sc.textFile(base + "user_movies.csv")
    // <movieID>,<评分>,<电影名>
    val rawHotMoviesData = sc.textFile(base + "hot_movies.csv")

    //准备数据
    preparation(rawUserMoviesData, rawHotMoviesData)
    println("准备完数据")

//    model(sc, rawUserMoviesData, rawHotMoviesData)

//    evaluate(sc,rawUserMoviesData, rawHotMoviesData)

    recommend(sc, rawUserMoviesData, rawHotMoviesData, base)
  }

  /**
    * 准备数据
    */
  def preparation(rawUserMoviesData: RDD[String],
                  rawHotMoviesData: RDD[String]) = {
    // 用户id的基本的统计信息
    val userIDStats = rawUserMoviesData
      .map(_.split(',')(0).trim)
      .distinct()
      .zipWithUniqueId()
      .map(_._2.toDouble)
      .stats()
    // 电影id的基本的统计信息
    val itemIDStats = rawUserMoviesData
      .map(_.split(',')(1).trim.toDouble)
      .distinct().stats()
    println("用户id的基本的统计信息" + userIDStats)
    println("电影id的基本的统计信息" + itemIDStats)
    // 获取电影名和id映射的map
    val movieIdAndName = buildMovies(rawHotMoviesData)
    val (movieID, movieName) = movieIdAndName.head
    println(movieID + " -> " + movieName)
  }

  /**
    * 获取电影名和id映射的map
    *
    * 读取rawHotMoviesData，
    * 因为rawHotMoviesData的每一行是一条类似 20645098,8.2,小王子  的字符串，
    * 需要按照,分割，得到第一个值(电影id)和第三个值(电影名)
    */
  def buildMovies(rawHotMoviesData: RDD[String]): scala.collection.Map[Int, String] = {
    rawHotMoviesData.flatMap(line => {
      val tokens = line.split(',')
      if (tokens(0).isEmpty) {
        None
      } else {
        Some((tokens(0).toInt, tokens(2)))
      }
    }).collectAsMap()
  }

  /**
    * 用户对电影的评分对象封装
   */
  def buildUserMovieRatings(rawUserMoviesData: RDD[String]): RDD[MovieRating] = {
    rawUserMoviesData.map { line =>
      val Array(userID, moviesID, countStr) = line.split(',').map(_.trim)
      var count = countStr.toInt
      count = if (count == -1) 3 else count
      MovieRating(userID, moviesID.toInt, count)
    }
  }

  def model(sc: SparkContext,
            rawUserMoviesData: RDD[String],
            rawHotMoviesData: RDD[String]): Unit = {
    val moviesAndName = buildMovies(rawHotMoviesData)
    val bMoviesAndName = sc.broadcast(moviesAndName)
    val data = buildUserMovieRatings(rawUserMoviesData)
    //  为每个用户id 都分配唯一索引  => (50249730,0)
    val userIdToInt: RDD[(String, Long)] = data.map(_.userID).distinct().zipWithUniqueId()
    //  (0,50249730)
    val reverseUserIDMapping: RDD[(Long, String)] = userIdToInt.map({ case (l, r) => (r, l) })

    //    println(data.collect().mkString(" "))
    //    println(userIdToInt.collect().mkString(" "))
    //    println(reverseUserIDMapping.collect().mkString(" "))
    val userIDMap = userIdToInt.collectAsMap().map { case (n, l) => (n, l.toInt) }
    val bUserIDMap = sc.broadcast(userIDMap)
    val ratings: RDD[Rating] = data.map { r => Rating(bUserIDMap.value(r.userID), r.movieID, r.rating) }.cache()
    //使用协同过滤算法建模
    //val model = ALS.trainImplicit(ratings, 10, 10, 0.01, 1.0)
    val model = ALS.train(ratings, 50, 10, 0.0001)
    ratings.unpersist()
    println("输出第一个userFeature")
    println(model.userFeatures.mapValues(_.mkString(", ")).first())
    for (userID <- Array(100, 1001, 10001, 100001, 110000)) {
      checkRecommendResult(userID, rawUserMoviesData, bMoviesAndName, reverseUserIDMapping, model)
    }
    //    unpersist(model)
  }

  /**
    * 评价模型
    * 我们可以通过计算均方差（Mean Squared Error, MSE）来衡量模型的好坏。
    * 数理统计中均方误差是指参数估计值与参数真值之差平方的期望值，记为MSE。
    * MSE是衡量“平均误差”的一种较方便的方法，MSE可以评价数据的变化程度，MSE的值越小，说明预测模型描述实验数据具有更好的精确度。
    *
    * 我们可以调整rank，numIterations，lambda，alpha这些参数，不断优化结果，使均方差变小。
    * 比如：iterations越多，lambda较小，均方差会较小，推荐结果较优。
    */
  def evaluate( sc: SparkContext,
                rawUserMoviesData: RDD[String],
                rawHotMoviesData: RDD[String]): Unit = {
    val moviesAndName = buildMovies(rawHotMoviesData)
    val data = buildUserMovieRatings(rawUserMoviesData)

    val userIdToInt: RDD[(String, Long)] =
      data.map(_.userID).distinct().zipWithUniqueId()


    val userIDMap: scala.collection.Map[String, Int] =
      userIdToInt.collectAsMap().map { case (n, l) => (n, l.toInt) }

    val bUserIDMap = sc.broadcast(userIDMap)

    val ratings: RDD[Rating] = data.map { r =>
      Rating(bUserIDMap.value(r.userID), r.movieID, r.rating)
    }.cache()

    val numIterations = 10
    // 评估显性反馈的参数的结果
    for (rank   <- Array(10,  50); lambda <- Array(1.0, 0.01,0.0001)) {
      val model = ALS.train(ratings, rank, numIterations, lambda)

      // Evaluate the model on rating data
      val usersMovies = ratings.map { case Rating(user, movie, rate) =>
        (user, movie)
      }
      val predictions =
        model.predict(usersMovies).map { case Rating(user, movie, rate) =>
          ((user, movie), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, movie, rate) =>
        ((user, movie), rate)
      }.join(predictions)

      val MSE = ratesAndPreds.map { case ((user, movie), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
      println(s"(rank:$rank, lambda: $lambda, Explicit ) Mean Squared Error = " + MSE)
    }

    //评估隐性反馈的参数的结果。
    for (rank   <- Array(10,  50); lambda <- Array(1.0, 0.01,0.0001); alpha  <- Array(1.0, 40.0)) {
      val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

      // Evaluate the model on rating data
      val usersMovies = ratings.map { case Rating(user, movie, rate) =>
        (user, movie)
      }
      val predictions =
        model.predict(usersMovies).map { case Rating(user, movie, rate) =>
          ((user, movie), rate)
        }
      val ratesAndPreds = ratings.map { case Rating(user, movie, rate) =>
        ((user, movie), rate)
      }.join(predictions)

      val MSE = ratesAndPreds.map { case ((user, movie), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
      }.mean()
      println(s"(rank:$rank, lambda: $lambda,alpha:$alpha ,implicit  ) Mean Squared Error = " + MSE)
    }
  }

  /**
    * 挑选几个用户，查看这些用户看过的电影，以及这个模型推荐给他们的电影
    */
  def checkRecommendResult(userID: Int,
                           rawUserMoviesData: RDD[String],
                           bMoviesAndName: Broadcast[scala.collection.Map[Int, String]],
                           reverseUserIDMapping: RDD[(Long, String)],
                           model: MatrixFactorizationModel): Unit = {

    val userName = reverseUserIDMapping.lookup(userID).head

    val recommendations = model.recommendProducts(userID, 5)
    //给此用户的推荐的电影ID集合
    val recommendedMovieIDs = recommendations.map(_.product).toSet

    //得到用户点播的电影ID集合
    val rawMoviesForUser = rawUserMoviesData.map(_.split(',')).
      filter { case Array(user, _, _) => user.trim == userName }
    val existingUserMovieIDs = rawMoviesForUser.map { case Array(_, movieID, _) => movieID.toInt }.
      collect().toSet


    println("=============用户" + userName + "点播过的电影名=============")
    //点播的电影名
    bMoviesAndName.value.filter { case (id, name) => existingUserMovieIDs.contains(id) }.values.foreach(println)

    println("=============推荐给用户" + userName + "的电影名=============")
    //推荐的电影名
    bMoviesAndName.value.filter { case (id, name) => recommendedMovieIDs.contains(id) }.values.foreach(println)
  }


  def recommend(sc: SparkContext,
                rawUserMoviesData: RDD[String],
                rawHotMoviesData: RDD[String],
                base: String): Unit = {
    val moviesAndName = buildMovies(rawHotMoviesData)
    val bMoviesAndName = sc.broadcast(moviesAndName)

    val data = buildUserMovieRatings(rawUserMoviesData)

    val userIdToInt: RDD[(String, Long)] =
      data.map(_.userID).distinct().zipWithUniqueId()
    val reverseUserIDMapping: RDD[(Long, String)] =
      userIdToInt map { case (l, r) => (r, l) }

    val userIDMap: scala.collection.Map[String, Int] =
      userIdToInt.collectAsMap().map { case (n, l) => (n, l.toInt) }

    val bUserIDMap = sc.broadcast(userIDMap)
    val bReverseUserIDMap = sc.broadcast(reverseUserIDMapping.collectAsMap())

    val ratings: RDD[Rating] = data.map { r =>
      Rating(bUserIDMap.value(r.userID), r.movieID, r.rating)
    }.cache()
    //使用协同过滤算法建模
    //val model = ALS.trainImplicit(ratings, 10, 10, 0.01, 1.0)
    val model = ALS.train(ratings, 50, 10, 0.0001)
    ratings.unpersist()

    model.save(sc, base + "model")
    //val sameModel = MatrixFactorizationModel.load(sc, base + "model")

    val allRecommendations = model.recommendProductsForUsers(5) map {
      case (userID, recommendations) => {
        var recommendationStr = ""
        for (r <- recommendations) {
          recommendationStr += r.product + ":" + bMoviesAndName.value.getOrElse(r.product, "") + ","
        }
        if (recommendationStr.endsWith(","))
          recommendationStr = recommendationStr.substring(0, recommendationStr.length - 1)

        (bReverseUserIDMap.value(userID), recommendationStr)
      }
    }

    // 将推荐结果写入到文件中。
    // 第一个字段是用户名，后面是五个推荐的电影(电影ID:电影名字)
    //allRecommendations.saveAsTextFile(base + "result.csv")
    allRecommendations.coalesce(1).sortByKey().saveAsTextFile(base + "result.csv")

    unpersist(model)
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    // At the moment, it's necessary to manually unpersist the RDDs inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }
}
