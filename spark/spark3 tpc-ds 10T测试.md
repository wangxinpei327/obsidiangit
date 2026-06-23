1. 使用kyuubi任务执行一段一时间后报错：
![[Pasted image 20260618092346.png|697]]

![[Pasted image 20260618092401.png]]
![[Pasted image 20260618092410.png]]
2. 开启了DRA、ESS，前面的executor可以正常传输kyuubi-spark-sql-engine.jar这个jar包，后面的executor不行。使用spark-sql可以正常启动。
3. 关闭DRA、ESS后，将executor instance数量限制到500后，kyuubi可以启动。
4. executor数量调上去之后，或者开了多个并发，也不行，单纯使用spark-sql启动任务也会报错。
5. 待查vmstat情况：
最关键指标：

```
steal time
```

例如：

```
st=20%
```

意味着：

```
20%时间CPU被宿主机抢走
```

再看上下文切换

```
vmstat 1
```

如果：

```
cs > 100000
```

或者：

```
r >> cpu核数
```

说明：

```
CPU调度已经炸了
```