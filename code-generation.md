## 简介
Spark SQL 支持代码生成技术，它最主要的目的是加速SQL的执行。在执行层面，之前非常流行的一种思路是每一个算子实现next方法，由最上层的算子调用它的子算子的next方法一层一层的进行处理，这种模型称之为volcano模型。volcano模型最大的好处就是实现简单，功能强大，对磁盘的IO影响较小。它的缺点主要是执行过程中会涉及到大量的虚函数调用，从而大大降低查询性能。代码生成技术的思想是将待执行的任务生成Java字节码，字节码里面没有虚函数调用，这样就可以大大加快执行速度。

## 源码分析
既然有代码生成，那么一定有代码的执行，我们先从代码执行这个比较简单的点入手Spark SQL的源码,之后再说明表达式代码生成和全阶段代码生成。

## CodeGeneration代码编译执行
Spark SQL使用janino来编译生成的代码。janino是一个小型的编译器，可以编译表达式，类，下面用一个小例子来说明它的使用：
1. 引入pom依赖
```
<dependency>
    <groupId>org.codehaus.janino</groupId>
    <artifactId>janino</artifactId>
    <version>3.0.15</version>
</dependency>
```

2. 定义接口IHello，它有一个hello的抽象方法:
```
public abstract class IHello {
    public abstract void hello();
}
```

3. 新建一个待编译的代码文件，它的内容现在是写死的，而CodeGeneration生成的代码是确定的，这里只是用来演示janino的使用流程：
```
// code.java
public void hello() {
    System.out.println("hello,world");
}
```
> 注意，在这里不要定义public class... 生成的类名、import的包名都可以在后面的代码指定，这里只需要写类里面的代码就可以了

4. 主函数：
```
public class Test  {
    public static void main(String[] args) throws CompileException, InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException, IOException {
        ClassBodyEvaluator ce = new ClassBodyEvaluator();
        ce.setExtendedClass(IHello.class);
        ce.cook(new FileInputStream("code.java"));
        Class clazz = ce.getClazz();
        IHello obj = (IHello)clazz.getConstructor().newInstance();
        obj.hello();
    }
}
```

运行结果:
```
hello,world
```

需要说明的是这里janino只是帮我们把代码编译成了class对象，通过getClazz这个方法可以得到它。这里也可以不用接口，直接通过反射来调hello方法也可以。

## Spark SQL 中的CodeGeneration代码编译和运行
这里使用spark2.3.2版本的spark进行分析。

object CodeGenerator里面有一个cache成员，它的key是代码，值对应的是编译的类。这样做的好处不言而喻：对于同一份代码，CodeGenerator只需要编译一次：
```
// CodeGenerator.scala
private val cache = CacheBuilder.newBuilder()
  .maximumSize(100)
  .build(
    new CacheLoader[CodeAndComment, (GeneratedClass, Int)]() {
      override def load(code: CodeAndComment): (GeneratedClass, Int) = {
        val startTime = System.nanoTime()
        val result = doCompile(code)
        val endTime = System.nanoTime()
        def timeMs: Double = (endTime - startTime).toDouble / 1000000
        CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
        CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
        logInfo(s"Code generated in $timeMs ms")
        result
      }
    })
```

编译的方法在doCompile里面实现，去掉了一些繁琐的细节：
```
private[this] def doCompile(code: CodeAndComment): (GeneratedClass, Int) = {
  val evaluator = new ClassBodyEvaluator()

  // ... set classLoader
  // set class name
  evaluator.setClassName("org.apache.spark.sql.catalyst.expressions.GeneratedClass")
  // import some classes...
  // set extended class 类似于上面的例子IHello
  evaluator.setExtendedClass(classOf[GeneratedClass])

  // get maxCodeSize
  val maxCodeSize = ....
  // 实例化对象
  (evaluator.getClazz().newInstance().asInstanceOf[GeneratedClass], maxCodeSize)
}
```
可以看到，和前面的例子思路大致类似，它扩展了接口GeneratedClass，也就是说，后面的代码生成需要实现这个方法
```
/**
 * A wrapper for generated class, defines a `generate` method so that we can pass extra objects
 * into generated class.
 */
abstract class GeneratedClass {
  def generate(references: Array[Any]): Any
}
```
