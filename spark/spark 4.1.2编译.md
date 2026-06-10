1、编译spark-tags报错，云服务器内存太小，修改pom.xml中xss\xmx（注意有两个）

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

core/pom.xml中添加：513         <exclusion>
514           <groupId>com.amazonaws</groupId>
515           <artifactId>aws-java-sdk-bundle</artifactId>
516         </exclusion>


4、编译spark connect时报错：[ERROR] /root/spark-4.1.2/sql/connect/common/src/main/protobuf/spark/connect/expressions.proto [0:0]: /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.8' not found 
(required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `GLIBCXX_3.4.21' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-
linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.9' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-li
nux-x86_64.exe)
--grpc-java_out: protoc-gen-grpc-java: Plugin failed with status code 1.

[ERROR] /root/spark-4.1.2/sql/connect/common/src/main/protobuf/spark/connect/commands.proto [0:0]: /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.8' not found (re
quired by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `GLIBCXX_3.4.21' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-
linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.9' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-li
nux-x86_64.exe)
--grpc-java_out: protoc-gen-grpc-java: Plugin failed with status code 1.

[ERROR] /root/spark-4.1.2/sql/connect/common/src/main/protobuf/spark/connect/relations.proto [0:0]: /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.8' not found (r
equired by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `GLIBCXX_3.4.21' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-
linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.9' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-li
nux-x86_64.exe)
--grpc-java_out: protoc-gen-grpc-java: Plugin failed with status code 1.

[ERROR] /root/spark-4.1.2/sql/connect/common/src/main/protobuf/spark/connect/pipelines.proto [0:0]: /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.8' not found (r
equired by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `GLIBCXX_3.4.21' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-
linux-x86_64.exe)
/root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.9' not found (required by /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-li
nux-x86_64.exe)
--grpc-java_out: protoc-gen-grpc-java: Plugin failed with status code 1.

~~升级GCC~~   
换成ubuntu 22.04系统进行编译