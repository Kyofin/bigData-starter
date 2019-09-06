object AdvanceFunc {

  def f(s: String) = "f(" + s + ")"

  def g(s: String) = "g(" + s + ")"

  def main(args: Array[String]): Unit = {
    // compose 函数组合
    val fComposeG = f _ compose g _
    println(fComposeG("yay"))



    // PartialFunction(偏函数)可以接收任意Int值，并返回一个字符串
    val one: PartialFunction[Int, String] = {
      case 1 => "one"
      case 2 => "two"
    }
    // one
    println(one(1))
    // Exception in thread "main" scala.MatchError: 3 (of class java.lang.Integer)
//    println(one(3))
    val wildcard :PartialFunction[Int,String] = {
      case _ => "something else"
    }
    // something else
    println(wildcard(99))
    // 组合one 和 wildcard 的case
    val partial = one orElse wildcard
    println(partial(3))

  }
}
