## 编译spark-tags报错，云服务器内存太小，修改pom.xml中xss\xmx（注意有两个）

## 部分包ali maven镜像没有，更换到原生maven 镜像

## 报错：[ERROR] Failed to execute goal org.apache.maven.plugins:maven-enforcer-plugin:3.6.2:enforce (enforce-versions) on project spark-core_2.13: 
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


## 编译spark connect时报错：[ERROR] /root/spark-4.1.2/sql/connect/common/src/main/protobuf/spark/connect/expressions.proto [0:0]: /root/spark-4.1.2/sql/connect/common/target/protoc-plugins/protoc-gen-grpc-java-1.76.0-linux-x86_64.exe: /lib64/libstdc++.so.6: version `CXXABI_1.3.8' not found 
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

## 换了个ubuntu系统：org.apache.maven.lifecycle.LifecycleExecutionException: Failed to execute goal net.alchim31.maven:scala-maven-plu 
gin:4.9.5:doc-jar (attach-scaladocs) on project spark-catalyst_2.13: MavenReportException: Error while creating a
rchive: wrap: Process exited with an error: 137 (Exit value: 137)                                                
    at org.apache.maven.lifecycle.internal.MojoExecutor.doExecute2 (MojoExecutor.java:333)                       
    at org.apache.maven.lifecycle.internal.MojoExecutor.doExecute (MojoExecutor.java:316)                        
    at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:212)                          
    at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:174)               
    at org.apache.maven.lifecycle.internal.MojoExecutor.access$000 (MojoExecutor.java:75)                        
    at org.apache.maven.lifecycle.internal.MojoExecutor$1.run (MojoExecutor.java:162)                            
    at org.apache.maven.plugin.DefaultMojosExecutionStrategy.execute (DefaultMojosExecutionStrategy.java:39)     
    at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:159)      
    at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject (LifecycleModuleBuilder.java:105) 
    at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject (LifecycleModuleBuilder.java:73)
    at org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder.build (SingleThreadedBuil
der.java:53)                                                                                                     
    at org.apache.maven.lifecycle.internal.LifecycleStarter.execute (LifecycleStarter.java:118)
    at org.apache.maven.DefaultMaven.doExecute (DefaultMaven.java:261)
    at org.apache.maven.DefaultMaven.doExecute (DefaultMaven.java:173)
    at org.apache.maven.DefaultMaven.execute (DefaultMaven.java:101)
    at org.apache.maven.cli.MavenCli.execute (MavenCli.java:906)
    at org.apache.maven.cli.MavenCli.doMain (MavenCli.java:283)
    at org.apache.maven.cli.MavenCli.main (MavenCli.java:206)
    at jdk.internal.reflect.NativeMethodAccessorImpl.invoke0 (Native Method)
    at jdk.internal.reflect.NativeMethodAccessorImpl.invoke (NativeMethodAccessorImpl.java:77)
    at jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke (DelegatingMethodAccessorImpl.java:43)
    at java.lang.reflect.Method.invoke (Method.java:569)
    at org.codehaus.plexus.classworlds.launcher.Launcher.launchEnhanced (Launcher.java:255)
    at org.codehaus.plexus.classworlds.launcher.Launcher.launch (Launcher.java:201)
    at org.codehaus.plexus.classworlds.launcher.Launcher.mainWithExitCode (Launcher.java:361)
 137代表什么

Linux 中：

```
137 = 128 + 9
```

即：

```
SIGKILL (kill -9)
```

绝大多数情况下意味着：

```
OOM Killer
```

把进程杀掉了。

---

 验证方法
```
dmesg -T | grep -i kill
```
~~跳过文档生成阶段：```~~
~~-Dmaven.javadoc.skip=true~~
```
进一步降低xms\xmx
修改pom.xml中180为     <maven.scaladoc.skip>true</maven.scaladoc.skip>