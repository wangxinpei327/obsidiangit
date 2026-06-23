参考：[Running Spark on YARN - Spark 4.1.2 Documentation](https://spark.apache.org/docs/latest/running-on-yarn.html#configuring-different-jdks-for-spark-applications)
修改Spark-env.sh中JAVA_HOME+提交配置修改：
```
spark-submit \--conf spark.driverEnv.JAVA_HOME=/usr/java/jdk-17 \--conf spark.executorEnv.JAVA_HOME=/usr/java/jdk-17 \--conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/java/jdk-17
```
也可打成tar包，避免每个节点放置
```
$ export JAVA_HOME=/opt/openjdk-21
$ ./bin/spark-submit --class path.to.your.Class \
    --master yarn \
    --archives path/to/openjdk-21.tar.gz \
    --conf spark.yarn.appMasterEnv.JAVA_HOME=./openjdk-21.tar.gz/openjdk-21 \
    --conf spark.executorEnv.JAVA_HOME=./openjdk-21.tar.gz/openjdk-21 \
    <app jar> [app options]
```

修改JDK 21时报错：
![[Pasted image 20260623104227.png]]
这是 **Spark 3.3.4 本身不支持 JDK21** 导致的，不是配置问题。
Spark 官方兼容矩阵：

|Spark|JDK8|JDK11|JDK17|JDK21|
|---|---|---|---|---|
|3.3.x|√|√|√|×|
|3.4.x|√|√|√|×|
|3.5.x|√|√|√|部分支持|
|4.0+|×|√|√|√|
