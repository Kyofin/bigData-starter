import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random


/** Computes an approximation to pi
  *
  * 这里我们说一下如何求 Pi，其实就是一个抽样的过程，假想有一个 2 * 2 的正方形，我要在里面画一个圆(r = 1)，
  * 假想有一个点随机扔到正方形中(假设有 N 次)，那么恰好也在圆中的次数为(C)，如果 N 足够大，那么 C/N 逼近于
  * 圆的面积/正方形面积，也就是说 pi/4，那么 pi/4 = C/N, pi = 4*C/N.
  *
  * */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    //设置log级别
    spark.setLogLevel("WARN")
    val slices = if (args.length > 0) args(0).toInt else 2

    // slices 对应于 partition 个数，平均每个 partition 有 100000L 个元素
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow

    // 这里可以理解为一个循环，每个 partition 上循环次数为 100000L
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1 // random return double value between [0.0, 1.0], so random * 2 - 1 return value between [-1.0, 1.0]
    val y = random * 2 - 1

      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
