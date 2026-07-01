## spark4 读取hive3建立的orc表时报错：![[Pasted image 20260623171305.png]]
定位spark3 jar包中该方法在aircompressor-0.21中，spark4中的是aircompressor-2.0.3版本，尝试spark.sql.orc.impl=native spark.sql.hive.convertMetastoreOrc=true同样报错。
将该包降级后报错消失

## spark4读取hive4的表时报错，invalid  method name“get_table”
指定spark.sql.hive.metastore.jars path
spark.sql.hive.metastore.jars.paths 为file:///user/hdp/……standalone-metastore/*
spark.sql.hive.metastore.version 4.1.0



## 分别使用hive3、4的hive-site.conf放到spark/conf中可以正常访问对应的HMS，但是不能交叉访问，不放任何hive-site.xml到conf文件夹后，指定hive4的hms服务uri可以访问，但是指定hive3的不行
报错：could not connect to meta store using any of the uris provided
```
GSS initiate failed
```
主要是hive4需要hive.metastore.kerberos.keytab.file和hive.metastore.kerberos.principal

## hive版本切换基本方式：
放置hive3的hive-site.xml到spark/conf中，建立standalone-metastore文件夹存放hivelib下的jar包，默认链接hive3，通过--conf制定参数的形式，切换到hive4
![[Pasted image 20260701165530.png]]