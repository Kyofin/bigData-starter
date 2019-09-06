
/**
  * 类class的特点是：
  *   里无static类型，类里的属性和方法，必须通过new出来的对象来调用，所以有main主函数也没用。
  *
  * 而object的特点是：
  *   可以拥有属性和方法，且默认都是"static"类型，
  *   可以直接用object名直接调用属性和方法，不需要通过new出来的对象（也不支持）。
  *   object里的main函数式应用程序的入口。
  *   object和class有很多和class相同的地方，可以extends父类或Trait，
  *   但object不可以extends object，即object无法作为父类。
  */
object TestVoMain {
  def main(args: Array[String]): Unit = {
    // new一个对象
    val vo = new TestVo()
    println(vo.id)
    // 调用对象的say方法
    println(vo.say(Array(1)))
  }
}

class TestVo{
  // class 里的属性默认是private类型，object里的属性默认是static
  var id = 10
  var name = "pidan"
  var age = null

  def say(args: Array[Int]): Int = {
    println("hello"+name+ args(0))
    // 方法返回100000
   100000
  }
}
