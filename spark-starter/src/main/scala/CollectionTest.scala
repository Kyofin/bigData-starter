import scala.collection.mutable.ArrayBuffer

/**
  * 基本数据结构使用
  * https://twitter.github.io/scala_school/zh_cn/collections.html
  */
object CollectionTest {
  def main(args: Array[String]): Unit = {
    //Array 数组是有序的，可以包含重复项，并且可变。下标从0开始
    val numbers = Array(11, 22, 33, 22, 33)
    // 更新数组值
    numbers.update(0, 7)
    // 显示数组内容
    println(numbers.mkString(" and "))





    //ArrayBuffer 长度按需要变化的数组
    // 参考：https://www.cnblogs.com/sunddenly/p/4411564.html
    val numbersArrayBuffer = ArrayBuffer[Int]()
    // 用+=在尾端添加元素
    numbersArrayBuffer += 1
    // 在尾端添加多个元素，以括号包起来
    numbersArrayBuffer += (2, 3, 4, 5, 6)
    // 用++=操作符追加任何集合
    numbersArrayBuffer ++= numbers
    println(numbersArrayBuffer.mkString(" "))





    //List 列表是有序的，可以包含重复项，不可变。
    val list = List(8, 3, 4, 5)
    // List(8, 3, 4, 5)
    println(list)
    println(list(0))
    // Error:(27, 5) value update is not a member of List[Int]
    //    list(0) =10





    // Set 集合无序且不可包含重复项。
    val set = Set(1,1,1,1,1)
    // Set(1)
    println(set)





    //tuple 元组在不使用类的情况下，将元素组合起来形成简单的逻辑集合。
    val hostPort = ("localhost",8080)
    // (localhost,8080)
    println(hostPort)
    // 元组不能通过名称获取字段，而是使用位置下标来读取对象；
    // 而且这个下标基于1，而不是基于0。
    println(hostPort._2)
    // 在创建两个元素的元组时，可以使用特殊语法：->
    // (1,abc)
    println(1->"abc")




    //Map 映射
    val map = Map(1->"a",2->"b",3->"c",4->"d")
    // Map.get 使用 Option 作为其返回值，表示这个方法也许不会返回你请求的值。
    println(map.get(5).getOrElse("空"))


  }


}
