import scala.util.control.Breaks.{break, breakable}


/**
* 循环
*/
object BreakTest {
  def main(args: Array[String]): Unit = {
    // break
    breakable{
      // 这里指i=5到10 ，注意to
      for (i <- 5 to 10) {
        if (i ==8){
          break()
        }
        println(i)
      }
    }

    println()
    // 这里是0 到 9 ，注意until
    for ( i <- 0 until 10) {
      println(i)
    }

    println()
    // continue
    for (i <- 1 to 10) {
      breakable {
        if (i == 2) {
          break()
        }
        println(i)
      }
    }
  }
}
