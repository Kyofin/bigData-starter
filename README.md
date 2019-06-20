## 基本原理
RDD，弹性分布式数据集，即分布式的元素集合。
在spark中，对所有数据的操作不外乎是创建RDD、转化已有的RDD以及调用RDD操作进行求值。在这一切的背后，Spark会自动将RDD中的数据分发到集群中，并将操作并行化。
Spark中的RDD就是一个不可变的分布式对象集合。每个RDD都被分为多个分区，这些分区运行在集群中的不同节点上。RDD可以包含Python，Java，Scala中任意类型的对象，甚至可以包含用户自定义的对象。
用户可以使用两种方法创建RDD：读取一个外部数据集，或在驱动器程序中分发驱动器程序中的对象集合，比如list或者set。
RDD的转化操作都是惰性求值的，这意味着我们对RDD调用转化操作，操作不会立即执行。相反，Spark会在内部记录下所要求执行的操作的相关信息。我们不应该把RDD看做存放着特定数据的数据集，而最好把每个RDD当做我们通过转化操作构建出来的、记录如何计算数据的指令列表。数据读取到RDD中的操作也是惰性的，数据只会在必要时读取。转化操作和读取操作都有可能多次执行。


## 代码样例
Spark入门（四）--Spark的map、flatMap、mapToPair:
https://juejin.im/post/5c77e383f265da2d8f474e29#heading-9

官方例子：
https://github.com/apache/spark/tree/master/examples/src/main/java/org/apache/spark/examples

## 常用api
1. map(func):对每行数据使用func，然后返回一个新的RDD,数据处理-每行。
2. filter(func):对每行数据使用func，然后返回func后为true的数据，用于过滤。
3. flatMap(func):和map差不多，但是flatMap生成的是多个结果，用于行转列。
4. groupByKey(numTasks):返回(K,Seq[V])，也就是Hadoop中reduce函数接受的
key-valuelist
5. reduceByKey(func,[numTasks]):就是用一个给定的reduce func再作用在groupByKey产
生的(K,Seq[V]),比如求和，求平均数
6. sortByKey([ascending],[numTasks]):按照key来进行排序，是升序还是降序，ascending


## 本地测试和提交作业
参考：https://blog.csdn.net/dream_an/article/details/54915894

- idea上测试spark作业

引入依赖
```
       <dependency> 
       <!-- Spark dependency -->
           <groupId>org.apache.spark</groupId>
           <artifactId>spark-core_2.11</artifactId>
           <version>2.4.0</version>
       </dependency>

       <!--不加上会出现org/glassfish/jersey/server/spi/Container not found-->
       <dependency>
           <groupId>org.glassfish.jersey.core</groupId>
           <artifactId>jersey-server</artifactId>
           <version>2.0-m03</version>
       </dependency>
```

- 提交作业到本机的spark环境

将使用`mvn clean package`打包好的作业提交到本地安装好的spark上跑
```
~/opt/spark-2.4.0-bin-hadoop2.7 » bin/spark-submit --class "com.wugui.sparkstarter.SimpleApp" /Users/huzekang/study/spark-starter/target/spark-starter-1.0-SNAPSHOT.jar

```
![](https://raw.githubusercontent.com/huzekang/picbed/master/20190620155332.png)

- 提交作业到yarn