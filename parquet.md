### 数据结构

- rowsReturned
- columnarBatch

### RecordReader 类
RecordReader类描述了读取记录的一些接口，由hadoop包提供，包括：

-   void initialize(InputSplit,TaskAttemptContext) 初始化reader
-   boolean nextKeyValue() 读取下一条记录，如果读取成功，则返回true
-   KEYIN getCurrentKey()  读取key
-   VALUEIN getCurrentValue() 读取value
-   float getProgress() 获取进度
-   void close() 关闭reader


### returnColumnarBatch 函数
VectorizedParquetRecordReader 继承至 SpecificParquetRecordReaderBase，它提供了Batch模式与ROW模式两种读取方法。通过returnColumnarBatch可以得到是否开启Batch模式，通过enableReturningBatches可以设置Batch,默认是ROW模式。

那么 Batch模式和ROW模式有什么区别呢？

区别主要在于 getCurrentKey
```
public Boolean nextKeyValue() throws IOException, InterruptedException {
	resultBatch();
	if (returnColumnarBatch) return nextBatch();
	if (batchIdx >= numBatched) {
		if (!nextBatch()) return false;
	}
	++batchIdx;
	return true;
}
```
对于Row模式来说，只有第一次读取数据的时候，才会真正去读一个Batch，其他情况下只是修改了batchIdx，然后如果当前的数据读完了，会再去读一个新的Batch。

VectorizedParquetRecordReader在实现nextKeyValue的时候，使用的value泛型是Object,这样就可以根据Batch或者Row模式返回不同的类型，在实现getCurrentValue时，如果是Batch模式，返回的就是ColumnarBatch，而Row模式下则是 ColumnarBatch.Row 类型：

```
public Object getCurrentValue() throws IOException, InterruptedException {
	if (returnColumnarBatch) return columnarBatch;
	return columnarBatch.getRow(batchIdx - 1);
}
```

### nextBatch 函数
可以看到，无论是什么模式下，最后的读取逻辑实际上是落在了nextBatch上：

```
/**
 * Advances to the next batch of rows. Returns false if there are no more.
 */
public Boolean nextBatch() throws IOException {
	columnarBatch.reset();
	if (rowsReturned >= totalRowCount) return false;
	checkEndOfRowGroup();
	int num = (int) Math.min((long) columnarBatch.capacity(), totalCountLoadedSoFar - rowsReturned);
	for (int i = 0; i < columnReaders.length; ++i) {
		if (columnReaders[i] == null) continue;
		columnReaders[i].readBatch(num, columnarBatch.column(i));
	}
	rowsReturned += num;
	columnarBatch.setNumRows(num);
	numBatched = num;
	batchIdx = 0;
	return true;
}
```

代码比较简单，大概分为3步：
1. 准备工作：将columnarBatch重置，判断当前读取的总行数是否超过最大行数
2. 读取：载入新的RowGroup,计算当前需要读人的行，按列读入
3. 收尾工作：更新已读的Row数、batchIdx（实现Row模式）等等

这里的最大行数是通过parquet文件的footer算出来的，我们后面会讲到。

### checkEndOfRowGroup 函数
```
private void checkEndOfRowGroup() throws IOException {
	if (rowsReturned != totalCountLoadedSoFar) return;
	PageReadStore pages = reader.readNextRowGroup();
	if (pages == null) {
		throw new IOException("expecting more rows but reached last block. Read "
		          + rowsReturned + " out of " + totalRowCount);
	}
	List<ColumnDescriptor> columns = requestedSchema.getColumns();
	columnReaders = new VectorizedColumnReader[columns.size()];
	for (int i = 0; i < columns.size(); ++i) {
		if (missingColumns[i]) continue;
		columnReaders[i] = new VectorizedColumnReader(columns.get(i),
		          pages.getPageReader(columns.get(i)));
	}
	totalCountLoadedSoFar += pages.getRowCount();
}
```
checkEndOfRowGroup做的事情主要是调用原生的parquet api读取rowgroup,然后会根据当前想读的列生成reader，最后比较重要的是，会更新totalCountLoadedSoFar来表示已经载入的RowGroup值。

