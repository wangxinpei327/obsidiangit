1. 使用kyuubi任务执行一段一时间后报错：
![[Pasted image 20260618092346.png|697]]

![[Pasted image 20260618092401.png]]
![[Pasted image 20260618092410.png]]
- 开启了DRA、ESS，前面的executor可以正常传输kyuubi-spark-sql-engine.jar这个jar包，后面的executor不行。使用spark-sql可以正常启动。
- 关闭DRA、ESS后，将executor instance数量限制到500后，kyuubi可以启动。
- executor数量调上去之后，或者开了多个并发，也不行，单纯使用spark-sql启动任务也会报错。

3. 200个executor，单executor调整至2core，作业大批量报错，cpu告警
4. 200个executor，400partition下，开启AQE与不开启差异不大
5. 降低了io.thread相关参数和maxreqsinflights到16，提升executor 到500，作业大批量报错
6. 下一步替换JDK并分别调整executor数量和partitions数量
7. 提高core的数量，单executor分32个core

8. 2026.7.27调用iceberg tpcds测试后，提交机器宕机，怀疑可能是有异常broadcast?
修改autobroadcast，并且降低driver端内存、关闭该节点nm、dn服务、关闭spark.sql.catalog.iceberg.cache-enabled=false，开启该选项后，Spark Catalog ，会缓存：
- Table Metadata
- Snapshot
- Schema
- Partition Spec
连续大量表访问，可能导致，Driver Heap 增长。

