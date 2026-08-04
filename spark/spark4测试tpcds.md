1. 使用spark4+kyuubi1.11启动测试后，大任务失败报错，查看日志中大量executor dead相关日志
![[Pasted image 20260803171034.png]]![[Pasted image 20260803171043.png]]
![[Pasted image 20260803171049.png]]
![[Pasted image 20260803171111.png]]
直接使用spark-sql提交并无此问题，通过回退kyuubi配置+*将kyuubi-spark-sql-engine jar包分发至spark_home/jars及yarn-archive.tar.gz*后，该问题解决。

2. 使用appview手动安装kyuubi后，/var/run/目录下、/app/logs下权限缺失：指定pid目录至/app/logs/kyuubi11，并将kyuubi11目录chown为appview用户
3. q32.sql中的cast需换为try_cast，否则校验报错
4. 