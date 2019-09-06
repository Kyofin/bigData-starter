import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
/**
  * scala用Java里面的类库
  *
  */
object UseJava {

  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val c = Calendar.getInstance()
    println(c.getTime)
    println(sdf.format(c.getTime))

    // Convert Code from Java，可以直接把Java代码贴到scala文件中来
    val sdf2: SimpleDateFormat = new SimpleDateFormat("yy-MM-dd")
    sdf2.format(new Date)
  }
}
