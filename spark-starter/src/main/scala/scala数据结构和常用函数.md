# 基本数据结构

Scala提供了一些不错的集合。

**参考** Effective Scala 对怎样使用[集合](https://twitter.github.com/effectivescala/#Collections)的观点。

## 数组 Array

数组是有序的，可以包含重复项，并且可变。

```scala
scala> val numbers = Array(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
numbers: Array[Int] = Array(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)

scala> numbers(3) = 10

scala> numbers
numbers: Array[Int] = Array(1, 2, 3, 10, 5, 1, 2, 3, 4, 5)
```

## 列表 List

列表是有序的，可以包含重复项，不可变。

```scala
scala> val numbers = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
numbers: List[Int] = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)

scala> numbers(3) = 10
<console>:9: error: value update is not a member of List[Int]
              numbers(3) = 10
```

## 集合 Set

集合无序且不可包含重复项。

```scala
scala> val numbers = Set(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
numbers: scala.collection.immutable.Set[Int] = Set(5, 1, 2, 3, 4)
```

## 元组 Tuple

元组在不使用类的情况下，将元素组合起来形成简单的逻辑集合。

```scala
scala> val hostPort = ("localhost", 80)
hostPort: (String, Int) = (localhost, 80)
```

与样本类不同，元组不能通过名称获取字段，而是使用位置下标来读取对象；而且这个下标基于1，而不是基于0。

```scala
scala> hostPort._1
res0: String = localhost

scala> hostPort._2
res1: Int = 80
```

元组可以很好得与模式匹配相结合。

```scala
hostPort match {
  case ("localhost", port) => ...
  case (host, port) => ...
}
```

在创建两个元素的元组时，可以使用特殊语法：`->`

```
scala> 1 -> 2
res0: (Int, Int) = (1,2)
```

**参考** Effective Scala 对 [解构绑定](https://twitter.github.com/effectivescala/#Functional programming-Destructuring bindings) （“拆解”一个元组）的观点。

## 映射 Map

它可以持有基本数据类型。

```scala
Map(1 -> 2)
Map("foo" -> "bar")
```

这看起来像是特殊的语法，不过不要忘了上文讨论的`->`可以用来创建二元组。

Map()方法也使用了从第一节课学到的变参列表：`Map(1 -> "one", 2 -> "two")`将变为 `Map((1, "one"), (2, "two"))`，其中第一个元素是映射的键，第二个元素是映射的值。

映射的值可以是映射甚至是函数。

```
Map(1 -> Map("foo" -> "bar"))
Map("timesTwo" -> { timesTwo(_) })
```

## 选项 Option

`Option` 是一个表示有可能包含值的容器。

Option基本的接口是这样的：

```scala
trait Option[T] {
  def isDefined: Boolean
  def get: T
  def getOrElse(t: T): T
}
```

Option本身是泛型的，并且有两个子类： `Some[T]` 或 `None`

我们看一个使用Option的例子：

`Map.get` 使用 `Option` 作为其返回值，表示这个方法也许不会返回你请求的值。

```scala
scala> val numbers = Map("one" -> 1, "two" -> 2)
numbers: scala.collection.immutable.Map[java.lang.String,Int] = Map(one -> 1, two -> 2)

scala> numbers.get("two")
res0: Option[Int] = Some(2)

scala> numbers.get("three")
res1: Option[Int] = None
```

现在我们的数据似乎陷在`Option`中了，我们怎样获取这个数据呢？

直觉上想到的可能是基于`isDefined`方法进行条件判断。

```scala
// We want to multiply the number by two, otherwise return 0.
val result = if (res1.isDefined) {
  res1.get * 2
} else {
  0
}
```

我们建议使用`getOrElse`或模式匹配处理这个结果。

`getOrElse` 让你轻松地定义一个默认值。

```scala
val result = res1.getOrElse(0) * 2
```

模式匹配能自然地配合`Option`使用。

```scala
val result = res1 match {
  case Some(n) => n * 2
  case None => 0
}
```

**参考** Effective Scala 对使用[Options](https://twitter.github.com/effectivescala/#Functional programming-Options)的意见。

# 函数组合子（Functional Combinators）

`List(1, 2, 3) map squared`对列表中的每一个元素都应用了`squared`平方函数，并返回一个新的列表`List(1, 4, 9)`。我们把类似于`map`的操作称作*组合子*。 （如果想要更好的定义，你可以看看Stackoverflow上对[组合子的说明](https://stackoverflow.com/questions/7533837/explanation-of-combinators-for-the-working-man)。）他们常被用在标准的数据结构上。

## map

`map`对列表中的每个元素应用一个函数，返回应用后的元素所组成的列表。

```scala
scala> val numbers = List(1, 2, 3, 4)
numbers: List[Int] = List(1, 2, 3, 4)

scala> numbers.map((i: Int) => i * 2)
res0: List[Int] = List(2, 4, 6, 8)
```

或传入一个函数 (Scala编译器自动把我们的方法转换为函数)

```scala
scala> def timesTwo(i: Int): Int = i * 2
timesTwo: (i: Int)Int

scala> numbers.map(timesTwo)
res0: List[Int] = List(2, 4, 6, 8)
```

## foreach

`foreach`很像map，但没有返回值。foreach仅用于有副作用[side-effects]的函数。

```scala
scala> numbers.foreach((i: Int) => i * 2)
```

什么也没有返回。

你可以尝试存储返回值，但它会是Unit类型（即void）

```scala
scala> val doubled = numbers.foreach((i: Int) => i * 2)
doubled: Unit = ()
```

## filter

`filter`移除任何对传入函数计算结果为false的元素。返回一个布尔值的函数通常被称为谓词函数[或判定函数]。

```scala
scala> numbers.filter((i: Int) => i % 2 == 0)
res0: List[Int] = List(2, 4)
scala> def isEven(i: Int): Boolean = i % 2 == 0
isEven: (i: Int)Boolean

scala> numbers.filter(isEven)
res2: List[Int] = List(2, 4)
```

## zip

`zip`将两个列表的内容聚合到一个对偶列表中。

```scala
scala> List(1, 2, 3).zip(List("a", "b", "c"))
res0: List[(Int, String)] = List((1,a), (2,b), (3,c))
```

## partition

`partition`将使用给定的谓词函数分割列表。

```scala
scala> val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
scala> numbers.partition(_ % 2 == 0)
res0: (List[Int], List[Int]) = (List(2, 4, 6, 8, 10),List(1, 3, 5, 7, 9))
```

## find

`find`返回集合中第一个匹配谓词函数的元素。

```scala
scala> numbers.find((i: Int) => i > 5)
res0: Option[Int] = Some(6)
```

## drop & dropWhile

`drop` 将删除前i个元素

```scala
scala> numbers.drop(5)
res0: List[Int] = List(6, 7, 8, 9, 10)
```

`dropWhile` 将删除匹配谓词函数的第一个元素。例如，如果我们在numbers列表上使用`dropWhile`函数来去除奇数, `1`将被丢弃（但`3`不会被丢弃，因为他被`2`“保护”了）。

```scala
scala> numbers.dropWhile(_ % 2 != 0)
res0: List[Int] = List(2, 3, 4, 5, 6, 7, 8, 9, 10)
```

## foldLeft

```scala
scala> numbers.foldLeft(0)((m: Int, n: Int) => m + n)
res0: Int = 55
```

0为初始值（记住numbers是List[Int]类型），m作为一个累加器。

可视化观察运行过程：

```scala
scala> numbers.foldLeft(0) { (m: Int, n: Int) => println("m: " + m + " n: " + n); m + n }
m: 0 n: 1
m: 1 n: 2
m: 3 n: 3
m: 6 n: 4
m: 10 n: 5
m: 15 n: 6
m: 21 n: 7
m: 28 n: 8
m: 36 n: 9
m: 45 n: 10
res0: Int = 55
```

### foldRight

和foldLeft一样，只是运行过程相反。

```scala
scala> numbers.foldRight(0) { (m: Int, n: Int) => println("m: " + m + " n: " + n); m + n }
m: 10 n: 0
m: 9 n: 10
m: 8 n: 19
m: 7 n: 27
m: 6 n: 34
m: 5 n: 40
m: 4 n: 45
m: 3 n: 49
m: 2 n: 52
m: 1 n: 54
res0: Int = 55
```

## flatten

`flatten`将嵌套结构扁平化一个层级。

```scala
scala> List(List(1, 2), List(3, 4)).flatten
res0: List[Int] = List(1, 2, 3, 4)
```

## flatMap

`flatMap`是一种常用的组合子，结合映射[mapping]和扁平化[flattening]。 flatMap需要一个处理嵌套列表的函数，然后将结果串连起来。

```scala
scala> val nestedNumbers = List(List(1, 2), List(3, 4))
nestedNumbers: List[List[Int]] = List(List(1, 2), List(3, 4))

scala> nestedNumbers.flatMap(x => x.map(_ * 2))
res0: List[Int] = List(2, 4, 6, 8)
```

可以把它看做是“先映射后扁平化”的快捷操作：

```scala
scala> nestedNumbers.map((x: List[Int]) => x.map(_ * 2)).flatten
res1: List[Int] = List(2, 4, 6, 8)
```

这个例子先调用map，然后调用flatten，这就是“组合子”的特征，也是这些函数的本质。

**参考** Effective Scala 对[flatMap](https://twitter.github.com/effectivescala/#Functional programming-`flatMap`)的意见。

## 扩展函数组合子

现在我们已经学过集合上的一些函数。

我们将尝试写自己的函数组合子。

有趣的是，上面所展示的每一个函数组合子都可以用fold方法实现。让我们看一些例子。

```scala
def ourMap(numbers: List[Int], fn: Int => Int): List[Int] = {
  numbers.foldRight(List[Int]()) { (x: Int, xs: List[Int]) =>
    fn(x) :: xs
  }
}

scala> ourMap(numbers, timesTwo(_))
res0: List[Int] = List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
```

为什么是`List[Int]()`？Scala没有聪明到理解你的目的是将结果积聚在一个空的Int类型的列表中。

## Map?

所有展示的函数组合子都可以在Map上使用。Map可以被看作是一个二元组的列表，所以你写的函数要处理一个键和值的二元组。

```scala
scala> val extensions = Map("steve" -> 100, "bob" -> 101, "joe" -> 201)
extensions: scala.collection.immutable.Map[String,Int] = Map((steve,100), (bob,101), (joe,201))
```

现在筛选出电话分机号码低于200的条目。

```scala
scala> extensions.filter((namePhone: (String, Int)) => namePhone._2 < 200)
res0: scala.collection.immutable.Map[String,Int] = Map((steve,100), (bob,101))
```

因为参数是元组，所以你必须使用位置获取器来读取它们的键和值。呃！

幸运的是，我们其实可以使用模式匹配更优雅地提取键和值。

```scala
scala> extensions.filter({case (name, extension) => extension < 200})
res0: scala.collection.immutable.Map[String,Int] = Map((steve,100), (bob,101))
```