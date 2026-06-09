1、编译spark-tags报错，云服务器内存太小，修改pom.xml中xss\xmx
2、部分包ali maven镜像没有，更换到原生maven 镜像

3、报错：[ERROR] Failed to execute goal org.apache.maven.plugins:maven-enforcer-plugin:3.6.2:enforce (enforce-versions) on project spark-core_2.13: 
[ERROR] Rule 2: org.apache.maven.enforcer.rules.dependency.BannedDependencies failed with message:
[ERROR] org.apache.spark:spark-core_2.13:jar:4.1.2
[ERROR]    org.apache.hadoop:hadoop-aws:jar:3.3.6
[ERROR]       com.amazonaws:aws-java-sdk-bundle:jar:1.12.367 <--- banned via the exclude/include list
[ERROR] 
[ERROR] -> [Help 1]
[ERROR]

core.pom中强制不引用aws相关包，跳过enforcer
```
./build/mvn \-Dhadoop.version=3.3.6 \-Denforcer.skip=true \-DskipTests
```