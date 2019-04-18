## 例子

我们先以一个WordCount小例子来开始本主题的说明：

```
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Test")
    val sc = new SparkContext(conf)
    val lines = sc.parallelize(Seq("i am a studuent", "i am a good boy"))
    val resultRDD = lines.flatMap(line => line.split(" ")).map(x => (x, 1)).reduceByKey(_+_).collect()
    println(resultRDD.mkString(","))
  }
}

output:
(a,2),(am,2),(i,2),(boy,1),(good,1),(studuent,1)
```

这里的 **reduceByKey** 会创建一个ShuffleRDD,而在调用了 **collect** 这个 action后，Job才真正运行。关于RDD及action相关的概念，可以参考[这里](http://spark.apache.org/docs/latest/rdd-programming-guide.html)的官方文档。本文侧重于shuffle相关模块的源码分析，不对其他模块进行展开。

### reduceByKey函数
