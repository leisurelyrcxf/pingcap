使用基于a列的划分，并行读文件，每个inMemoryDivide都会并行执行，如果内存不够，会缓存到文件系统
内存大小通过maxAvailableMemory进行限制，默认为256MB，可以按照实际情况进行设置

共计用时7小时左右



当输入量很大时，
如果group数量很小，(a,b)去重后数量很小，则可以将数据都放入内存，适当增大桶的数目,增大defaultInMemoryDivide，需要将代码中的memoryConflateRate设为一个较小的值

如果group数量很大，(a,b)去重后数量也很大，如果数据不能全放入内存，此时就只能缓存到文件系统，如果内存可以放m个桶，总共的分桶树为totalMemoryNeeded/availableMemory * m.
这样划分时前m个桶可以直接在内存中建立哈希表并并行执行，思想类似于hybrid hash-join算法。后续每m个桶并行执行一次即可（需要从文件系统读）。

由于题目涉及的计算较为简单，因此主要的瓶颈在io上。基本上性能取决于并行读取文件并完成划分所用的时间
