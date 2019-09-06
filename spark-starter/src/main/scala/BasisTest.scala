

object BasisTest {

  case class Calculator(brand: String, model: String)

  def main(args: Array[String]): Unit = {
    // 变量声明用var
    var name = "steve"
    name = "peter"

    // 不变量声明用val
    val age =12
//    age =11


    // 模式匹配
    println(calTimes(1))
    println(calTimes2(4))

    // 样本类 Case Classes。使用样本类可以方便得存储和匹配类的内容。不用new关键字就可以创建它们。
    val hp20b = Calculator("HP", "20b")
    println(hp20b)
    // 使用样本类进行模式匹配
    println(calcuType(Calculator("HP", "20B")))


  }

  def calcuType(calc: Calculator) = calc match {
    case Calculator("HP", "20B") => "financial"
    case Calculator("HP", "48G") => "scientific"
    case Calculator("HP", "30B") => "business"
    case Calculator(ourBrand, ourModel) => "Calculator: %s %s is of unknown type".format(ourBrand, ourModel)

  }

  // 1. 匹配值
  def calTimes(times: Int): String = times match {
    case 1 => "one"
    case 2 => "two"
    case 3 => "three"
    case _ => "something else"
  }

  // 2. 使用守卫匹配
  def calTimes2(times: Int): String = times match {
    case i if i == 1 => "one"
    case i if i == 2 => "two"
    case i if i == 3 => "three"
    case _ => "something else"
  }

}
