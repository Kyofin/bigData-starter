
### 代码样例
https://juejin.im/post/5c77e383f265da2d8f474e29#heading-9

### 本地测试和提交作业
参考：https://blog.csdn.net/dream_an/article/details/54915894

- idea上测试spark作业

- 提交作业到本地spark
将使用`mvn clean package`打包好的作业提交到本地安装好的spark上跑
```
~/opt/spark-2.4.0-bin-hadoop2.7 » bin/spark-submit --class "com.wugui.sparkstarter.SimpleApp" /Users/huzekang/study/spark-starter/target/spark-starter-1.0-SNAPSHOT.jar

```
![](https://raw.githubusercontent.com/huzekang/picbed/master/20190620155332.png)

- 提交作业到yarn