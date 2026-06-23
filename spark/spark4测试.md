## spark4 读取hive3建立的orc表时报错：![[Pasted image 20260623171305.png]]
定位spark3 jar包中该方法在aircompressor-0.21中，spark4中的是aircompressor-2.0.3版本，尝试spark.sql.orc.impl=native spark.sql.hive.convertMetastoreOrc=true同样报错。
将该包降级后报错消失


