object FuncTest {
  def main(args: Array[String]): Unit = {
    // map 对列表中的每个元素应用一个函数，返回应用后的元素所组成的列表。
    val numbers = List(1, 2, 3, 4)
    val numbersMap = numbers.map((a: Int) => a * 2)
    println(numbersMap)

    // zip 将两个列表的内容组合到一个tuple列表中
    val tuples = List(1, 2, 3).zip(List(4, 5, 6))
    println(tuples)

    // partition 将使用给定的谓词函数分割列表。
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(list.partition(a => a % 2 == 0))
    // 此处 _ 代表每个元素
    println(list.partition(_ % 2 == 0))

    // flatten 将嵌套结构扁平化一个层级。
    val list2 = List(List(1, 2, 3), List(4, 5, 6))
    // List(1, 2, 3, 4, 5, 6)
    println(list2.flatten)

    // flatMap是一种常用的组合子，结合映射[mapping]和扁平化[flattening]。
    // flatMap需要一个处理嵌套列表的函数，然后将结果串连起来。
    // 先调用map，然后调用flatten，这就是“组合子”的特征
    val nestNumbers = List(List(1, 2, 3), List(4, 5, 6))
    // 下面本质是相等的
    println(nestNumbers.flatMap(x => x.map(_ * 2)))
    println(nestNumbers.map(l => l.map(_ * 2)).flatten)

  }
}
