
/**
  * 伴生类
  */
class BangSheng {
  def add2(a: Int, b: Int): Int = {
    a + b
  }
}

object  BangSheng{
  def add(a:Int,b:Int) :Int ={
    a+b
  }

  def main(args: Array[String]): Unit = {
    println(BangSheng.add(1, 2))
    val bangSheng = new BangSheng
    println(bangSheng.add2(3, 4))
  }
}