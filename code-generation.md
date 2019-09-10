## 简介
Spark SQL 支持代码生成技术，它最主要的目的是加速SQL的执行。在执行层面，之前非常流行的一种思路是每一个算子实现next方法，由最上层的算子调用它的子算子的next方法一层一层的进行处理，这种模型称之为volcano模型。volcano模型最大的好处就是实现简单，功能强大，对磁盘的IO影响较小。它的缺点主要是执行过程中会涉及到大量的虚函数调用，从而大大降低查询性能。代码生成技术的思想是将待执行的任务生成Java字节码，字节码里面没有虚函数调用，这样就可以大大加快执行速度。

## 源码分析
既然有代码生成，那么一定有代码的编译，我们先从代码编译这个比较简单的点入手Spark SQL的源码,之后再说明表达式代码生成和全阶段代码生成。

## CodeGeneration代码编译
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

## Spark SQL 中的CodeGeneration代码编译
这里使用spark2.3.2版本的spark进行分析。

object CodeGenerator里面有一个cache成员，它的key是代码对象，值对应的是编译的类。这样做的好处不言而喻：对于同一份代码，CodeGenerator只需要编译一次：
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

## 表达式生成

复现代码
```
import org.apache.spark.sql._;
import org.apache.spark.sql.catalyst.expressions._;
import org.apache.spark.sql.catalyst._;
import org.apache.spark.sql.types._;

val schema = new StructType(Array(
  StructField("a", StringType, true),
  StructField("b", IntegerType, true),
  StructField("c", new StructType(Array(
    StructField("aa", StringType, true),
    StructField("bb", IntegerType, true)
  )), true),
  StructField("d", new StructType(Array(
    StructField("a", new StructType(Array(
      StructField("b", StringType, true),
      StructField("", IntegerType, true)
    )), true)
  )), true)
))
val row = Row("a", 1, Row("b", 2), Row(Row("c", 3)))
val lit = Literal.create(row, schema)
val internalRow = lit.value.asInstanceOf[InternalRow]

sc.setLogLevel("debug")
val unsafeProj = UnsafeProjection.create(schema)
```

```
/* 001 */ public java.lang.Object generate(Object[] references) {
/* 002 */   return new SpecificUnsafeProjection(references);
/* 003 */ }
/* 004 */
/* 005 */ class SpecificUnsafeProjection extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {
/* 006 */
/* 007 */   private Object[] references;
/* 008 */   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder[] mutableStateArray_1 = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder[1];
/* 009 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] mutableStateArray_2 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[4];
/* 010 */   private UnsafeRow[] mutableStateArray_0 = new UnsafeRow[1];
/* 011 */
/* 012 */   public SpecificUnsafeProjection(Object[] references) {
/* 013 */     this.references = references;
/* 014 */     mutableStateArray_0[0] = new UnsafeRow(4);
/* 015 */     mutableStateArray_1[0] = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(mutableStateArray_0[0], 96);
/* 016 */     mutableStateArray_2[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(mutableStateArray_1[0], 4);
/* 017 */     mutableStateArray_2[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(mutableStateArray_1[0], 2);
/* 018 */     mutableStateArray_2[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(mutableStateArray_1[0], 1);
/* 019 */     mutableStateArray_2[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(mutableStateArray_1[0], 2);
/* 020 */
/* 021 */   }
/* 022 */
/* 023 */   public void initialize(int partitionIndex) {
/* 024 */
/* 025 */   }
/* 026 */
/* 027 */   // Scala.Function1 need this
/* 028 */   public java.lang.Object apply(java.lang.Object row) {
/* 029 */     return apply((InternalRow) row);
/* 030 */   }
/* 031 */
/* 032 */   public UnsafeRow apply(InternalRow i) {
/* 033 */     mutableStateArray_1[0].reset();
/* 034 */
/* 035 */     mutableStateArray_2[0].zeroOutNullBytes();
/* 036 */     writeFields_0_0(i);
/* 037 */     writeFields_0_1(i);
/* 038 */     mutableStateArray_0[0].setTotalSize(mutableStateArray_1[0].totalSize());
/* 039 */     return mutableStateArray_0[0];
/* 040 */   }
/* 041 */
/* 042 */
/* 043 */   private void writeFields_0_1(InternalRow i) {
/* 044 */
/* 045 */
/* 046 */     boolean isNull_3 = i.isNullAt(3);
/* 047 */     InternalRow value_3 = isNull_3 ? null : (i.getStruct(3, 1));
/* 048 */     if (isNull_3) {
/* 049 */       mutableStateArray_2[0].setNullAt(3);
/* 050 */     } else {
/* 051 */       // Remember the current cursor so that we can calculate how many bytes are
/* 052 */       // written later.
/* 053 */       final int tmpCursor_5 = mutableStateArray_1[0].cursor;
/* 054 */
/* 055 */       final InternalRow tmpInput_1 = value_3;
/* 056 */       if (tmpInput_1 instanceof UnsafeRow) {
/* 057 */
/* 058 */         final int sizeInBytes_1 = ((UnsafeRow) tmpInput_1).getSizeInBytes();
/* 059 */         // grow the global buffer before writing data.
/* 060 */         mutableStateArray_1[0].grow(sizeInBytes_1);
/* 061 */         ((UnsafeRow) tmpInput_1).writeToMemory(mutableStateArray_1[0].buffer, mutableStateArray_1[0].cursor);
/* 062 */         mutableStateArray_1[0].cursor += sizeInBytes_1;
/* 063 */
/* 064 */       } else {
/* 065 */         mutableStateArray_2[2].reset();
/* 066 */
/* 067 */
/* 068 */         if (tmpInput_1.isNullAt(0)) {
/* 069 */           mutableStateArray_2[2].setNullAt(0);
/* 070 */         } else {
/* 071 */           // Remember the current cursor so that we can calculate how many bytes are
/* 072 */           // written later.
/* 073 */           final int tmpCursor_6 = mutableStateArray_1[0].cursor;
/* 074 */
/* 075 */           final InternalRow tmpInput_2 = tmpInput_1.getStruct(0, 2);
/* 076 */           if (tmpInput_2 instanceof UnsafeRow) {
/* 077 */
/* 078 */             final int sizeInBytes_2 = ((UnsafeRow) tmpInput_2).getSizeInBytes();
/* 079 */             // grow the global buffer before writing data.
/* 080 */             mutableStateArray_1[0].grow(sizeInBytes_2);
/* 081 */             ((UnsafeRow) tmpInput_2).writeToMemory(mutableStateArray_1[0].buffer, mutableStateArray_1[0].cursor);
/* 082 */             mutableStateArray_1[0].cursor += sizeInBytes_2;
/* 083 */
/* 084 */           } else {
/* 085 */             mutableStateArray_2[3].reset();
/* 086 */
/* 087 */
/* 088 */             if (tmpInput_2.isNullAt(0)) {
/* 089 */               mutableStateArray_2[3].setNullAt(0);
/* 090 */             } else {
/* 091 */               mutableStateArray_2[3].write(0, tmpInput_2.getUTF8String(0));
/* 092 */             }
/* 093 */
/* 094 */
/* 095 */             if (tmpInput_2.isNullAt(1)) {
/* 096 */               mutableStateArray_2[3].setNullAt(1);
/* 097 */             } else {
/* 098 */               mutableStateArray_2[3].write(1, tmpInput_2.getInt(1));
/* 099 */             }
/* 100 */           }
/* 101 */
/* 102 */           mutableStateArray_2[2].setOffsetAndSize(0, tmpCursor_6, mutableStateArray_1[0].cursor - tmpCursor_6);
/* 103 */         }
/* 104 */       }
/* 105 */
/* 106 */       mutableStateArray_2[0].setOffsetAndSize(3, tmpCursor_5, mutableStateArray_1[0].cursor - tmpCursor_5);
/* 107 */     }
/* 108 */
/* 109 */   }
/* 110 */
/* 111 */
/* 112 */   private void writeFields_0_0(InternalRow i) {
/* 113 */
/* 114 */
/* 115 */     boolean isNull_0 = i.isNullAt(0);
/* 116 */     UTF8String value_0 = isNull_0 ? null : (i.getUTF8String(0));
/* 117 */     if (isNull_0) {
/* 118 */       mutableStateArray_2[0].setNullAt(0);
/* 119 */     } else {
/* 120 */       mutableStateArray_2[0].write(0, value_0);
/* 121 */     }
/* 122 */
/* 123 */
/* 124 */     boolean isNull_1 = i.isNullAt(1);
/* 125 */     int value_1 = isNull_1 ? -1 : (i.getInt(1));
/* 126 */     if (isNull_1) {
/* 127 */       mutableStateArray_2[0].setNullAt(1);
/* 128 */     } else {
/* 129 */       mutableStateArray_2[0].write(1, value_1);
/* 130 */     }
/* 131 */
/* 132 */
/* 133 */     boolean isNull_2 = i.isNullAt(2);
/* 134 */     InternalRow value_2 = isNull_2 ? null : (i.getStruct(2, 2));
/* 135 */     if (isNull_2) {
/* 136 */       mutableStateArray_2[0].setNullAt(2);
/* 137 */     } else {
/* 138 */       // Remember the current cursor so that we can calculate how many bytes are
/* 139 */       // written later.
/* 140 */       final int tmpCursor_2 = mutableStateArray_1[0].cursor;
/* 141 */
/* 142 */       final InternalRow tmpInput_0 = value_2;
/* 143 */       if (tmpInput_0 instanceof UnsafeRow) {
/* 144 */
/* 145 */         final int sizeInBytes_0 = ((UnsafeRow) tmpInput_0).getSizeInBytes();
/* 146 */         // grow the global buffer before writing data.
/* 147 */         mutableStateArray_1[0].grow(sizeInBytes_0);
/* 148 */         ((UnsafeRow) tmpInput_0).writeToMemory(mutableStateArray_1[0].buffer, mutableStateArray_1[0].cursor);
/* 149 */         mutableStateArray_1[0].cursor += sizeInBytes_0;
/* 150 */
/* 151 */       } else {
/* 152 */         mutableStateArray_2[1].reset();
/* 153 */
/* 154 */
/* 155 */         if (tmpInput_0.isNullAt(0)) {
/* 156 */           mutableStateArray_2[1].setNullAt(0);
/* 157 */         } else {
/* 158 */           mutableStateArray_2[1].write(0, tmpInput_0.getUTF8String(0));
/* 159 */         }
/* 160 */
/* 161 */
/* 162 */         if (tmpInput_0.isNullAt(1)) {
/* 163 */           mutableStateArray_2[1].setNullAt(1);
/* 164 */         } else {
/* 165 */           mutableStateArray_2[1].write(1, tmpInput_0.getInt(1));
/* 166 */         }
/* 167 */       }
/* 168 */
/* 169 */       mutableStateArray_2[0].setOffsetAndSize(2, tmpCursor_2, mutableStateArray_1[0].cursor - tmpCursor_2);
/* 170 */     }
/* 171 */
/* 172 */   }
/* 173 */
/* 174 */ }
```
